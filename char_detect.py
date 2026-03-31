#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字符/组合字符存在性探测工具（多线程版）

用法:
  python char_detect.py <文件> -c "| ~ ^"           # 探测多个单字符
  python char_detect.py <文件> -c "~~ ^^ !~ ~!"     # 探测多个组合字符
  python char_detect.py <文件> -c "| ~~ !@"         # 混合探测
  python char_detect.py <文件> -c "~~" --full        # 全文件扫描
  python char_detect.py <文件> -c "~~" --workers 8
"""

import os
import sys
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

READ_BUF = 8 * 1024 * 1024   # 每块 8MB

# ── 进度 ──────────────────────────────────────────────────────────────────────
_lock       = threading.Lock()
_done_bytes = 0
_total_bytes = 0
_t0 = 0.0


def _add(n):
    global _done_bytes
    with _lock:
        _done_bytes += n


def _progress(stop: threading.Event):
    while not stop.is_set():
        with _lock:
            done = _done_bytes
        elapsed = time.time() - _t0 or 1e-6
        pct   = done / _total_bytes * 100 if _total_bytes else 0
        speed = done / elapsed / 1024 / 1024
        eta   = (_total_bytes - done) / (done / elapsed) if done > 0 else 0
        print(f"\r  {pct:5.1f}%  {speed:7.1f} MB/s  "
              f"{done/1024/1024:,.0f}/{_total_bytes/1024/1024:,.0f} MB  "
              f"剩余 {eta:.0f}s   ",
              end='', flush=True)
        stop.wait(0.5)


# ── 核心扫描：一个文件段，检测多个 pattern ────────────────────────────────────
def scan_segment(path: str, start: int, length: int,
                 patterns: list[bytes]) -> dict[bytes, int]:
    """
    扫描 [start, start+length)，返回每个 pattern 出现的次数。
    保留每块末尾 (max_pattern_len - 1) 字节拼入下一块，避免跨块漏检。
    """
    overlap = max(len(p) for p in patterns) - 1
    counts = {p: 0 for p in patterns}
    remaining = length
    tail = b''

    with open(path, 'rb') as f:
        f.seek(start)
        while remaining > 0:
            raw = f.read(min(READ_BUF, remaining))
            if not raw:
                break
            buf = tail + raw
            for p in patterns:
                counts[p] += buf.count(p)
            tail = raw[-overlap:] if overlap > 0 else b''
            _add(len(raw))
            remaining -= len(raw)

    return counts


# ── 主流程 ────────────────────────────────────────────────────────────────────
def main():
    global _done_bytes, _total_bytes, _t0

    parser = argparse.ArgumentParser(
        description='多线程字符/组合字符存在性探测',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('filepath',   help='目标文件路径')
    parser.add_argument('-c', '--chars', required=True,
                        help='要探测的字符或组合，空格分隔，如 "| ~~ !@"')
    parser.add_argument('--sample-mb', type=int, default=0,
                        help='采样 MB 数，默认 0 = 全文件')
    parser.add_argument('--full', action='store_true',
                        help='全文件扫描（默认已是全文件，可省略）')
    parser.add_argument('--workers', type=int, default=os.cpu_count() or 4,
                        help=f'线程数，默认 {os.cpu_count() or 4}')
    args = parser.parse_args()

    path = args.filepath
    if not os.path.exists(path):
        print(f"错误：文件不存在: {path}")
        sys.exit(1)

    # 解析待探测的 pattern 列表
    raw_patterns = args.chars.split()
    if not raw_patterns:
        print("错误：-c 参数为空")
        sys.exit(1)

    # 编码为 bytes（用 latin-1 保证单字节 ASCII 原样保留）
    try:
        patterns = [p.encode('latin-1') for p in raw_patterns]
    except UnicodeEncodeError as e:
        print(f"错误：字符编码失败（仅支持 ASCII/Latin-1 字符）: {e}")
        sys.exit(1)

    file_size = os.path.getsize(path)
    scan_size = (file_size if (args.full or args.sample_mb == 0)
                 else min(args.sample_mb * 1024 * 1024, file_size))
    workers   = max(1, min(args.workers, 32))
    is_sample = scan_size < file_size

    print()
    print(f"文件路径 : {path}")
    print(f"文件大小 : {file_size:,} 字节  ({file_size/1024**3:.2f} GB)")
    print(f"扫描范围 : {'全文件' if not is_sample else f'前 {scan_size//1024//1024} MB'}")
    print(f"线程数   : {workers}")
    print(f"探测目标 : {' '.join(repr(p) for p in raw_patterns)}")
    print()

    # 切分文件段
    seg  = scan_size // workers
    segs = [(i * seg, seg if i < workers - 1 else scan_size - i * seg)
            for i in range(workers)]

    # 启动进度线程
    _done_bytes  = 0
    _total_bytes = scan_size
    _t0 = time.time()
    stop = threading.Event()
    threading.Thread(target=_progress, args=(stop,), daemon=True).start()

    # 并发扫描
    totals: dict[bytes, int] = {p: 0 for p in patterns}
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(scan_segment, path, s, l, patterns): i
                    for i, (s, l) in enumerate(segs)}
            for fut in as_completed(futs):
                for p, cnt in fut.result().items():
                    totals[p] += cnt
    finally:
        stop.set()

    elapsed = time.time() - _t0
    speed   = scan_size / elapsed / 1024 / 1024
    print(f"\r  完成！耗时 {elapsed:.1f}s  平均速度 {speed:.1f} MB/s{' '*30}")
    print()

    # ── 输出结果 ──────────────────────────────────────────────────────────────
    note = f"（仅采样前 {scan_size//1024//1024} MB）" if is_sample else ""
    SEP  = '─' * 52

    absent  = [(r, p) for r, p in zip(raw_patterns, patterns) if totals[p] == 0]
    present = sorted([(r, p, totals[p]) for r, p in zip(raw_patterns, patterns)
                      if totals[p] > 0], key=lambda x: x[2])

    print(SEP)
    print(f"  ✅ 不存在于文件中（可安全用作分隔符）{note}")
    print(SEP)
    if absent:
        for r, p in absent:
            hex_s = ''.join(f'\\x{b:02x}' for b in p)
            print(f"  \033[32m{r:<8}  hex: {hex_s}\033[0m")
        print()
        best = absent[0][0]
        print(f"  推荐使用: {repr(best)}")
        print(f"  Python:  SEP = {repr(best)}")
        print(f"           fields = line.rstrip('\\n').split(SEP)")
    else:
        print("  所有探测目标均已存在于文件中")

    print()
    print(SEP)
    print("  ❌ 已存在于文件中（出现次数由少到多）")
    print(SEP)
    if present:
        for r, p, cnt in present:
            hex_s = ''.join(f'\\x{b:02x}' for b in p)
            print(f"  {r:<8}  hex: {hex_s}  出现 {cnt:>14,} 次{note}")
    else:
        print("  无")

    print()


if __name__ == '__main__':
    main()
