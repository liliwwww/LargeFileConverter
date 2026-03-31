#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
固定宽度多行报表 → 标准 CSV 转换脚本

每条记录跨5行，空行分隔：
  行1: 商户编号  商户名称  品牌编号  品牌名称  地区编号  地区名称
  行2: 详细地址
  行3: 所属行业  所属行业名称
  行4: 云闪付借记卡D1  云闪付借记卡D0  云闪付贷记卡D1  云闪付贷记卡D0  支付宝D0  支付宝D1  微信D0  微信D1
  行5: 借记卡D1封顶金额分(可空)  借记卡D1费率  借记卡D0封顶金额分(可空)  借记卡D0费率  贷记卡D1  贷记卡D0

用法:
  python convert_report.py input.txt output.csv
  python convert_report.py input.txt            # 自动生成 input.csv
"""

import re
import csv
import sys
import os

# ── 输出列名（顺序固定）────────────────────────────────────────────────────────
COLUMNS = [
    '商户编号', '商户名称', '品牌编号', '品牌名称', '地区编号', '地区名称',
    '详细地址',
    '所属行业', '所属行业名称',
    '云闪付借记卡D1', '云闪付借记卡D0', '云闪付贷记卡D1', '云闪付贷记卡D0',
    '支付宝D0', '支付宝D1', '微信D0', '微信D1',
    '借记卡D1封顶金额分', '借记卡D1费率', '借记卡D0封顶金额分', '借记卡D0费率',
    '贷记卡D1', '贷记卡D0',
]

LINES_PER_RECORD = 5  # 每条记录占5行（不含空行）


# ─────────────────────────────────────────────────────────────────────────────
# 编码检测
# ─────────────────────────────────────────────────────────────────────────────

def detect_encoding(path: str) -> str:
    try:
        import chardet
        with open(path, 'rb') as f:
            raw = f.read(200_000)
        enc = chardet.detect(raw).get('encoding') or 'utf-8'
        return {'GB2312': 'gbk', 'ascii': 'utf-8'}.get(enc, enc)
    except ImportError:
        pass
    for enc in ('utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                f.read(50_000)
            return enc
        except (UnicodeDecodeError, LookupError):
            pass
    return 'utf-8'


# ─────────────────────────────────────────────────────────────────────────────
# 分块：按空行分割
# ─────────────────────────────────────────────────────────────────────────────

def split_blocks(lines: list[str]) -> list[list[str]]:
    """将所有行按空行分割成若干块，每块是一条记录（或表头）。"""
    blocks, cur = [], []
    for line in lines:
        s = line.rstrip('\r\n')
        if s.strip() == '':
            if cur:
                blocks.append(cur)
                cur = []
        else:
            cur.append(s)
    if cur:
        blocks.append(cur)
    return blocks


# ─────────────────────────────────────────────────────────────────────────────
# 行解析工具
# ─────────────────────────────────────────────────────────────────────────────

def split_wide(s: str) -> list[str]:
    """按 2 个以上连续空格分割，过滤空串。"""
    return [t.strip() for t in re.split(r' {2,}', s.strip()) if t.strip()]


def parse_line5(line: str) -> list[str]:
    """
    解析第5行：借记卡封顶/费率 + 贷记卡费率，共6个字段。

    字段顺序：
      借记卡D1封顶金额分(整数,可空)  借记卡D1费率(小数)
      借记卡D0封顶金额分(整数,可空)  借记卡D0费率(小数)
      贷记卡D1(小数)  贷记卡D0(小数)

    关键：封顶金额分是整数(如2000,2500)，费率是小数(如.005,.0038)，
    利用值类型区分，不依赖列位置，可正确处理封顶为空的情况。
    """
    tokens = line.split()  # 按任意空白分割

    fields = ['', '', '', '', '', '']
    # fields[0]=借记卡D1封顶  [1]=借记卡D1费率  [2]=借记卡D0封顶
    # fields[3]=借记卡D0费率  [4]=贷记卡D1      [5]=贷记卡D0

    def is_cap(t: str) -> bool:
        """封顶金额分：纯整数（不含小数点）"""
        return t.isdigit()

    def is_rate(t: str) -> bool:
        """费率：包含小数点"""
        return '.' in t

    slot = 0  # 当前期望填入的槽位
    for token in tokens:
        if slot == 0:
            # 期望: 封顶D1(可选) 或 费率D1
            if is_cap(token):
                fields[0] = token   # 封顶D1 有值
                slot = 1
            elif is_rate(token):
                fields[0] = ''      # 封顶D1 为空，直接到费率D1
                fields[1] = token
                slot = 2
        elif slot == 1:
            # 期望: 费率D1
            fields[1] = token
            slot = 2
        elif slot == 2:
            # 期望: 封顶D0(可选) 或 费率D0
            if is_cap(token):
                fields[2] = token   # 封顶D0 有值
                slot = 3
            elif is_rate(token):
                fields[2] = ''      # 封顶D0 为空
                fields[3] = token
                slot = 4
        elif slot == 3:
            # 期望: 费率D0
            fields[3] = token
            slot = 4
        elif slot == 4:
            # 期望: 贷记卡D1
            fields[4] = token
            slot = 5
        elif slot == 5:
            # 期望: 贷记卡D0
            fields[5] = token
            break

    return fields


# ─────────────────────────────────────────────────────────────────────────────
# 记录块解析
# ─────────────────────────────────────────────────────────────────────────────

def parse_block(block: list[str], block_num: int) -> tuple[list[str] | None, str]:
    """
    解析一个5行记录块，返回 (字段列表, 警告信息)。
    字段列表共23个，对应 COLUMNS 顺序。
    返回 None 表示解析失败。
    """
    if len(block) < LINES_PER_RECORD:
        return None, f"第{block_num}块仅有{len(block)}行（期望{LINES_PER_RECORD}行）"

    warnings = []

    # ── 行1：商户编号 商户名称 品牌编号 品牌名称 地区编号 地区名称 ──────────────
    line1_fields = split_wide(block[0])
    if len(line1_fields) < 6:
        warnings.append(f"行1字段数={len(line1_fields)}(期望6)")
    row1 = (line1_fields + [''] * 6)[:6]

    # ── 行2：详细地址（整行）────────────────────────────────────────────────────
    row2 = [block[1].strip()]

    # ── 行3：所属行业 所属行业名称 ──────────────────────────────────────────────
    line3_fields = split_wide(block[2])
    if len(line3_fields) < 2:
        line3_fields = line3_fields + ['']
    row3 = (line3_fields + [''] * 2)[:2]

    # ── 行4：8个支付费率 ─────────────────────────────────────────────────────────
    line4_fields = split_wide(block[3])
    if len(line4_fields) < 8:
        warnings.append(f"行4字段数={len(line4_fields)}(期望8)")
    row4 = (line4_fields + [''] * 8)[:8]

    # ── 行5：借记卡封顶/费率 + 贷记卡费率 ────────────────────────────────────────
    row5 = parse_line5(block[4])

    combined = row1 + row2 + row3 + row4 + row5
    warn_str = '; '.join(warnings) if warnings else ''
    return combined, warn_str


# ─────────────────────────────────────────────────────────────────────────────
# 主转换逻辑
# ─────────────────────────────────────────────────────────────────────────────

def is_header_block(block: list[str]) -> bool:
    """判断是否是表头块（第一块，包含'商户编号'等文字）。"""
    first_line = block[0] if block else ''
    return '商户编号' in first_line or '商户名称' in first_line


def convert(input_path: str, output_path: str):
    print(f"输入文件: {input_path}")

    encoding = detect_encoding(input_path)
    print(f"检测编码: {encoding}")

    with open(input_path, encoding=encoding, errors='replace') as f:
        lines = f.readlines()

    blocks = split_blocks(lines)
    print(f"共分割出 {len(blocks)} 个块")

    success = skip = warn_count = 0

    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)

        for i, block in enumerate(blocks, start=1):
            # 跳过表头块
            if is_header_block(block):
                print(f"  块#{i}: 表头，已跳过")
                continue

            if len(block) < LINES_PER_RECORD:
                print(f"  块#{i}: 行数不足{LINES_PER_RECORD}行，已跳过 → {block[0][:40]}")
                skip += 1
                continue

            row, warning = parse_block(block, i)
            if row is None:
                print(f"  块#{i}: 解析失败，已跳过")
                skip += 1
                continue

            if warning:
                print(f"  块#{i}: 警告 [{warning}] → 商户={row[0]}")
                warn_count += 1

            writer.writerow(row)
            success += 1

    print(f"\n转换完成:")
    print(f"  成功: {success} 条")
    print(f"  跳过: {skip} 条")
    print(f"  警告: {warn_count} 条")
    print(f"输出文件: {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"错误：文件不存在: {input_path}")
        sys.exit(1)

    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        base = os.path.splitext(input_path)[0]
        output_path = base + '.csv'

    convert(input_path, output_path)
