#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大文件分隔符探测工具（多线程版）
每个线程独立 seek 到文件不同区段并发读取，速度随线程数线性提升（SSD）。

用法:
  python find_delimiter.py <文件路径>
  python find_delimiter.py <文件路径> --sample-mb 100   # 只扫前100MB
  python find_delimiter.py <文件路径> --full             # 全文件扫描
  python find_delimiter.py <文件路径> --workers 8        # 指定线程数
"""

import os
import sys
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── 候选单字符分隔符（半角英文，按常用优先级排列）────────────────────────────────
CANDIDATES = [
    (0x7C, '|',  '竖线'),
    (0x09, '\t', 'Tab'),
    (0x5E, '^',  '脱字符'),
    (0x7E, '~',  '波浪线'),
    (0x60, '`',  '反引号'),
    (0x3B, ';',  '分号'),
    (0x23, '#',  '井号'),
    (0x40, '@',  'At符号'),
    (0x21, '!',  '感叹号'),
    (0x25, '%',  '百分号'),
    (0x26, '&',  'And符号'),
    (0x2A, '*',  '星号'),
    (0x2B, '+',  '加号'),
    (0x3D, '=',  '等号'),
    (0x2C, ',',  '逗号'),
    (0x2F, '/',  '斜杠'),
    (0x5C, '\\', '反斜杠'),
]

# ── ASCII 控制字符（专为数据分隔设计，正常文本极少出现）──────────────────────────
CTRL_CANDIDATES = [
    (0x01, r'\x01', 'SOH 标题开始'),
    (0x02, r'\x02', 'STX 正文开始'),
    (0x03, r'\x03', 'ETX 正文结束'),
    (0x04, r'\x04', 'EOT 传输结束'),
    (0x1C, r'\x1C', 'FS  文件分隔符'),
    (0x1D, r'\x1D', 'GS  组分隔符'),
    (0x1E, r'\x1E', 'RS  记录分隔符'),  # ← 最推荐，专为此设计
    (0x1F, r'\x1F', 'US  单元分隔符'),  # ← 最推荐，专为此设计
]

ALL_CANDIDATES = CANDIDATES + CTRL_CANDIDATES

CANDIDATE_BYTES = bytes([b for b, _, _ in ALL_CANDIDATES])

# 每个字节的"可读显示名"，用于组合输出
_BYTE_DISPLAY = {b: (r'\t' if ch == '\t' else ch) for b, ch, _ in ALL_CANDIDATES}
_BYTE_DISPLAY.update({b: desc.split()[0] for b, _, desc in CTRL_CANDIDATES})
# e.g. 0x01 → 'SOH', 0x1F → 'US'

def combo_display(cb: bytes) -> str:
    """把双字节组合转成可读字符串，可见字符直接显示，控制字符用名称。"""
    parts = []
    for b in cb:
        if 0x20 <= b <= 0x7E:          # 可打印 ASCII
            parts.append(chr(b))
        elif b == 0x09:
            parts.append(r'\t')
        else:
            parts.append(_BYTE_DISPLAY.get(b, f'\\x{b:02x}'))
    # 如果两部分都是单字符可见字符，显示为  'xy'
    # 否则显示为  左+右
    if all(len(p) == 1 for p in parts):
        return f"'{''.join(parts)}'"
    return '+'.join(parts)
READ_BUF = 8 * 1024 * 1024  # 每次读取 8MB

# ── 进度追踪 ──────────────────────────────────────────────────────────────────
_progress_lock = threading.Lock()
_bytes_done = 0
_total_bytes = 0
_start_time = 0.0


def _update_progress(n: int):
    global _bytes_done
    with _progress_lock:
        _bytes_done += n


def _progress_printer(stop_event: threading.Event):
    """后台线程：每秒刷新一次进度。"""
    while not stop_event.is_set():
        elapsed = time.time() - _start_time
        with _progress_lock:
            done = _bytes_done
        pct = done / _total_bytes * 100 if _total_bytes else 0
        speed = done / elapsed / 1024 / 1024 if elapsed > 0 else 0
        eta = (_total_bytes - done) / (done / elapsed) if done > 0 else 0
        print(f"\r  进度: {pct:5.1f}%  速度: {speed:6.1f} MB/s  "
              f"已读: {done/1024/1024:.0f}/{_total_bytes/1024/1024:.0f} MB  "
              f"剩余: {eta:.0f}s    ",
              end='', flush=True)
        stop_event.wait(1.0)


# ── 单线程任务：扫描文件的 [start, start+length) 字节段（单字符计数）──────────
def scan_segment(file_path: str, start: int, length: int) -> dict:
    """返回各候选字节在本段中的出现次数。"""
    counts = {b: 0 for b, _, _ in ALL_CANDIDATES}
    remaining = length

    with open(file_path, 'rb') as f:
        f.seek(start)
        while remaining > 0:
            chunk = f.read(min(READ_BUF, remaining))
            if not chunk:
                break
            for byte_val in counts:
                counts[byte_val] += chunk.count(bytes([byte_val]))
            _update_progress(len(chunk))
            remaining -= len(chunk)

    return counts


# ── 单线程任务：扫描文件段中是否包含指定的双字节组合 ────────────────────────────
def scan_combos_segment(file_path: str, start: int, length: int,
                        combos: list[bytes]) -> dict:
    """
    检测多个双字节组合是否出现在 [start, start+length) 段中。
    返回 {combo_bytes: count}。
    关键：每次读取时在 chunk 前拼接上一块末尾 1 字节，避免跨块漏检。
    """
    found = {c: 0 for c in combos}
    remaining = length
    prev_tail = b''

    with open(file_path, 'rb') as f:
        f.seek(start)
        while remaining > 0:
            chunk = f.read(min(READ_BUF, remaining))
            if not chunk:
                break
            # 拼接上一块末尾，确保跨块边界的组合不被遗漏
            buf = prev_tail + chunk
            for combo in combos:
                found[combo] += buf.count(combo)
            prev_tail = chunk[-1:]  # 保留最后 1 字节
            _update_progress(len(chunk))
            remaining -= len(chunk)

    return found


# ── 主流程 ────────────────────────────────────────────────────────────────────
def main():
    global _bytes_done, _total_bytes, _start_time

    parser = argparse.ArgumentParser(description='大文件分隔符探测工具（多线程）')
    parser.add_argument('filepath', help='目标文件路径')
    parser.add_argument('--sample-mb', type=int, default=20,
                        help='采样大小 MB，默认20MB；0=全文件扫描')
    parser.add_argument('--full', action='store_true',
                        help='全文件扫描（等同于 --sample-mb 0）')
    parser.add_argument('--workers', type=int, default=os.cpu_count() or 4,
                        help=f'并发线程数，默认={os.cpu_count() or 4}')
    parser.add_argument('--top-n', type=int, default=6,
                        help='取出现次数最少的前N个单字符参与组合探测，默认6')
    args = parser.parse_args()

    path = args.filepath
    if not os.path.exists(path):
        print(f"错误：文件不存在: {path}")
        sys.exit(1)

    file_size = os.path.getsize(path)
    sample_mb = 0 if args.full else args.sample_mb
    scan_size = file_size if sample_mb == 0 else min(sample_mb * 1024 * 1024, file_size)
    workers = max(1, min(args.workers, 32))
    is_sample = scan_size < file_size

    print()
    print(f"文件路径 : {path}")
    print(f"文件大小 : {file_size:,} 字节  ({file_size / 1024**3:.2f} GB)")
    print(f"扫描范围 : {'全文件' if not is_sample else f'前 {scan_size/1024/1024:.0f} MB'}")
    print(f"线程数   : {workers}")
    print()

    # 把扫描范围均分给每个线程
    segment_size = scan_size // workers
    segments = []
    for i in range(workers):
        start = i * segment_size
        length = segment_size if i < workers - 1 else scan_size - start
        segments.append((start, length))

    # 启动进度打印线程
    _total_bytes = scan_size
    _bytes_done = 0
    _start_time = time.time()
    stop_event = threading.Event()
    progress_thread = threading.Thread(target=_progress_printer,
                                        args=(stop_event,), daemon=True)
    progress_thread.start()

    # 并发扫描
    total_counts = {b: 0 for b, _, _ in ALL_CANDIDATES}
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(scan_segment, path, start, length): i
                for i, (start, length) in enumerate(segments)
            }
            for future in as_completed(futures):
                seg_counts = future.result()
                for b, cnt in seg_counts.items():
                    total_counts[b] += cnt
    finally:
        stop_event.set()
        progress_thread.join()

    elapsed = time.time() - _start_time
    speed = scan_size / elapsed / 1024 / 1024
    print(f"\r  Phase1 完成！耗时 {elapsed:.1f}s，平均速度 {speed:.1f} MB/s{' '*20}")
    print()

    # ── Phase 2：组合字符扫描 ──────────────────────────────────────────────────
    # 取出现次数最少的前 top_n 个单字符（含出现次数为0的），生成所有双字符组合
    top_n = args.top_n
    sorted_all = sorted(ALL_CANDIDATES, key=lambda x: total_counts[x[0]])
    top_chars = sorted_all[:top_n]

    # 生成组合：同字符重复(xx) + 两两组合(xy/yx)
    combo_list: list[tuple[bytes, str]] = []
    seen = set()
    for i, (b1, ch1, _) in enumerate(top_chars):
        for j, (b2, ch2, _) in enumerate(top_chars):
            key = (b1, b2)
            if key in seen:
                continue
            seen.add(key)
            cb = bytes([b1, b2])
            display1 = r'\t' if ch1 == '\t' else ch1
            display2 = r'\t' if ch2 == '\t' else ch2
            combo_list.append((cb, f'{display1}{display2}'))

    print(f"Phase 2：探测 {len(combo_list)} 种双字符组合...")
    _bytes_done = 0
    _total_bytes = scan_size * len(combo_list) // max(len(combo_list), 1)
    # 实际上 Phase2 总读量 = scan_size，多个 combo 在同一次读取中一起检测
    _total_bytes = scan_size
    _bytes_done = 0
    _start_time = time.time()

    # 每段同时检测所有 combo（一次 I/O，多个 pattern）
    combo_counts: dict[bytes, int] = {cb: 0 for cb, _ in combo_list}
    stop_event2 = threading.Event()
    progress_thread2 = threading.Thread(target=_progress_printer,
                                         args=(stop_event2,), daemon=True)
    progress_thread2.start()
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures2 = {
                executor.submit(scan_combos_segment, path, start, length,
                                [cb for cb, _ in combo_list]): i
                for i, (start, length) in enumerate(segments)
            }
            for future in as_completed(futures2):
                seg = future.result()
                for cb, cnt in seg.items():
                    combo_counts[cb] += cnt
    finally:
        stop_event2.set()
        progress_thread2.join()

    elapsed2 = time.time() - _start_time
    print(f"\r  Phase2 完成！耗时 {elapsed2:.1f}s{' '*40}")
    print()

    # ── 输出结果 ──────────────────────────────────────────────────────────────
    safe_single = [(b, ch, desc) for b, ch, desc in CANDIDATES     if total_counts[b] == 0]
    safe_ctrl   = [(b, ch, desc) for b, ch, desc in CTRL_CANDIDATES if total_counts[b] == 0]
    unsafe      = [(b, ch, desc, total_counts[b]) for b, ch, desc in ALL_CANDIDATES if total_counts[b] > 0]
    unsafe.sort(key=lambda x: x[3])

    sep = "=" * 60

    # ── 安全单字符 ────────────────────────────────────────────────────────────
    print(sep)
    print("  ✅ 推荐：可见单字符分隔符（文件中不存在）")
    print(sep)
    if safe_single:
        for _, ch, desc in safe_single:
            display = r'\t(Tab)' if ch == '\t' else ch
            print(f"  \033[32m{display:<10} {desc}\033[0m")
    else:
        print("  无可用的可见单字符")

    # ── 安全控制字符 ──────────────────────────────────────────────────────────
    print()
    print(sep)
    print("  ✅ 推荐：ASCII 控制字符分隔符（文件中不存在）")
    print("     专为数据分隔设计，不可见但被 Python/Java/数据库广泛支持")
    print(sep)
    if safe_ctrl:
        for _, ch, desc in safe_ctrl:
            print(f"  \033[36m{ch:<10} {desc}\033[0m")
        print()
        print("  使用示例（Python）:")
        best = safe_ctrl[0][1]
        print(f"    delimiter = '{best}'   # {best} = {safe_ctrl[0][2]}")
        print(f"    csv.reader(f, delimiter=delimiter)")
    else:
        print("  控制字符也全部存在于文件中（非常罕见）")

    # ── 组合分隔符结果 ────────────────────────────────────────────────────────
    safe_combos   = [(cb, disp) for cb, disp in combo_list if combo_counts[cb] == 0]
    unsafe_combos = sorted(
        [(cb, disp, combo_counts[cb]) for cb, disp in combo_list if combo_counts[cb] > 0],
        key=lambda x: x[2]
    )

    def is_visible_combo(cb: bytes) -> bool:
        return all(0x20 <= b <= 0x7E or b == 0x09 for b in cb)

    safe_visible = [(cb, disp) for cb, disp in safe_combos if is_visible_combo(cb)]
    safe_ctrl_c  = [(cb, disp) for cb, disp in safe_combos if not is_visible_combo(cb)]

    print()
    print(sep)
    print("  ✅ 推荐：可见字符组合（文件中不存在，直接可读）")
    print(sep)
    if safe_visible:
        for cb, _ in safe_visible:
            label = combo_display(cb)
            hex_str = ''.join(f'\\x{b:02x}' for b in cb)
            print(f"  \033[32m{label:<12}  hex: {hex_str}\033[0m")
        print()
        best_cb = safe_visible[0][0]
        best_label = combo_display(best_cb)
        best_sep = best_cb.decode('latin-1')
        print("  使用示例（Python）:")
        print(f"    SEP = {repr(best_sep)}")
        print(f"    fields = line.rstrip('\\n').split(SEP)")
    else:
        print("  无可见字符组合可用")

    print()
    print(sep)
    print("  ✅ 备选：控制字符组合（文件中不存在，不可见但可靠）")
    print(sep)
    if safe_ctrl_c:
        for cb, _ in safe_ctrl_c[:8]:   # 最多显示8个
            label = combo_display(cb)
            hex_str = ''.join(f'\\x{b:02x}' for b in cb)
            print(f"  \033[36m{label:<14}  hex: {hex_str}\033[0m")
        if len(safe_ctrl_c) > 8:
            print(f"  ... 共 {len(safe_ctrl_c)} 个控制字符组合可用")
        print()
        best_ctrl = safe_ctrl_c[0][0]
        print("  使用示例（Python）:")
        print(f"    SEP = {repr(best_ctrl.decode('latin-1'))}")
        print(f"    fields = line.rstrip('\\n').split(SEP)")
    else:
        print("  无控制字符组合可用")

    if not safe_combos:
        print()
        print("  所有双字符组合均存在于文件，出现最少的前5个：")
        for cb, disp, cnt in unsafe_combos[:5]:
            label = combo_display(cb)
            note = "（采样）" if is_sample else ""
            print(f"  {label:<14}  出现 {cnt:,} 次{note}")
    print()

    # ── 所有字符出现次数 ──────────────────────────────────────────────────────
    print(sep)
    print("  📊 所有候选字符出现统计（由少到多）")
    print(sep)
    for _, ch, desc, cnt in unsafe:
        sample_note = "（采样）" if is_sample else ""
        print(f"  {ch:<10} {desc:<16} 出现 {cnt:>14,} 次{sample_note}")

    print()
    if is_sample:
        print("  提示：当前为采样扫描，建议加 --full 全文件确认后再决定")


if __name__ == '__main__':
    main()
