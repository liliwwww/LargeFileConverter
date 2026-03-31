#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双字符可见分隔符探测工具（多线程版）

从指定的可见字符集中，生成所有双字符组合（含重复如 !!），
多线程并发扫描文件，找出文件中完全不存在的组合作为分隔符候选。

用法:
  python find_combo_delimiter.py <文件路径>
  python find_combo_delimiter.py <文件路径> --full          # 全文件扫描
  python find_combo_delimiter.py <文件路径> --sample-mb 100
  python find_combo_delimiter.py <文件路径> --workers 8
"""

import os
import sys
import time
import argparse
import itertools
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── 候选可见字符集（按用户指定）─────────────────────────────────────────────────
VISIBLE_CHARS = list('!=&')

# 生成所有双字符组合（含自身重复：!! `` == ...，共 14×14=196 种）
# 使用 product 而非 combinations，因为 AB 和 BA 是不同的分隔符
ALL_COMBOS: list[bytes] = [
    (a + b).encode('latin-1')
    for a, b in itertools.product(VISIBLE_CHARS, repeat=2)
]

READ_BUF = 8 * 1024 * 1024  # 每次读取 8MB

# ── 进度追踪 ──────────────────────────────────────────────────────────────────
_lock        = threading.Lock()
_bytes_done  = 0
_total_bytes = 0
_start_time  = 0.0


def _add_progress(n: int):
    global _bytes_done
    with _lock:
        _bytes_done += n


def _show_progress(stop: threading.Event):
    while not stop.is_set():
        with _lock:
            done = _bytes_done
        elapsed = time.time() - _start_time
        pct   = done / _total_bytes * 100 if _total_bytes else 0
        speed = done / elapsed / 1024 / 1024 if elapsed > 0.001 else 0
        eta   = (_total_bytes - done) / (done / elapsed) if done > 0 else 0
        print(f"\r  进度: {pct:5.1f}%  速度: {speed:6.1f} MB/s  "
              f"已扫: {done/1024/1024:,.0f} / {_total_bytes/1024/1024:,.0f} MB  "
              f"预计剩余: {eta:.0f}s   ",
              end='', flush=True)
        stop.wait(1.0)


# ── 核心：单段扫描 ─────────────────────────────────────────────────────────────
def scan_segment(file_path: str, start: int, length: int,
                 combos: list[bytes]) -> dict[bytes, int]:
    """
    扫描文件 [start, start+length) 字节段，统计每个双字符组合出现次数。
    关键：保留每块末尾 1 字节拼入下一块，避免跨块边界漏检。
    """
    counts = {c: 0 for c in combos}
    remaining = length
    prev_tail = b''

    with open(file_path, 'rb') as f:
        f.seek(start)
        while remaining > 0:
            raw = f.read(min(READ_BUF, remaining))
            if not raw:
                break
            buf = prev_tail + raw          # 拼接上块末尾，覆盖跨块边界
            for combo in combos:
                counts[combo] += buf.count(combo)
            prev_tail = raw[-1:]           # 保留最后 1 字节
            _add_progress(len(raw))
            remaining -= len(raw)

    return counts


# ── 主流程 ────────────────────────────────────────────────────────────────────
def main():
    global _bytes_done, _total_bytes, _start_time

    parser = argparse.ArgumentParser(description='双字符可见分隔符探测（多线程）')
    parser.add_argument('filepath', help='目标文件路径')
    parser.add_argument('--sample-mb', type=int, default=20,
                        help='采样大小 MB，默认 20MB；0 = 全文件')
    parser.add_argument('--full', action='store_true',
                        help='全文件扫描（等同于 --sample-mb 0）')
    parser.add_argument('--workers', type=int, default=os.cpu_count() or 4,
                        help=f'线程数，默认 {os.cpu_count() or 4}')
    args = parser.parse_args()

    path = args.filepath
    if not os.path.exists(path):
        print(f"错误：文件不存在: {path}")
        sys.exit(1)

    file_size = os.path.getsize(path)
    sample_mb = 0 if args.full else args.sample_mb
    scan_size = file_size if sample_mb == 0 else min(sample_mb * 1024 * 1024, file_size)
    workers   = max(1, min(args.workers, 32))
    is_sample = scan_size < file_size

    n_combos = len(ALL_COMBOS)
    print()
    print(f"文件路径   : {path}")
    print(f"文件大小   : {file_size:,} 字节  ({file_size / 1024**3:.2f} GB)")
    print(f"扫描范围   : {'全文件' if not is_sample else f'前 {scan_size/1024/1024:.0f} MB'}")
    print(f"线程数     : {workers}")
    print(f"候选字符   : {''.join(VISIBLE_CHARS)}")
    print(f"组合总数   : {n_combos} 种  ({len(VISIBLE_CHARS)} × {len(VISIBLE_CHARS)})")
    print()

    # 均分文件段给各线程
    seg_size = scan_size // workers
    segments = [
        (i * seg_size,
         seg_size if i < workers - 1 else scan_size - i * seg_size)
        for i in range(workers)
    ]

    # 启动进度线程
    _bytes_done  = 0
    _total_bytes = scan_size
    _start_time  = time.time()
    stop_evt = threading.Event()
    threading.Thread(target=_show_progress, args=(stop_evt,), daemon=True).start()

    # 并发扫描
    total: dict[bytes, int] = {c: 0 for c in ALL_COMBOS}
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {
                ex.submit(scan_segment, path, s, l, ALL_COMBOS): i
                for i, (s, l) in enumerate(segments)
            }
            for fut in as_completed(futs):
                for cb, cnt in fut.result().items():
                    total[cb] += cnt
    finally:
        stop_evt.set()

    elapsed = time.time() - _start_time
    speed   = scan_size / elapsed / 1024 / 1024
    print(f"\r  完成！耗时 {elapsed:.1f}s，平均速度 {speed:.1f} MB/s{' ' * 30}")
    print()

    # ── 结果分类 ──────────────────────────────────────────────────────────────
    safe   = [cb for cb in ALL_COMBOS if total[cb] == 0]
    unsafe = sorted([(cb, total[cb]) for cb in ALL_COMBOS if total[cb] > 0],
                    key=lambda x: x[1])

    sep = '=' * 62
    note = "（采样）" if is_sample else ""

    # ── 安全组合 ──────────────────────────────────────────────────────────────
    print(sep)
    print(f"  ✅ 可用组合：文件中完全不存在（共 {len(safe)} 个）")
    print(sep)

    if safe:
        # 按"是否是两个相同字符"排序：相同的放前面（更易记）
        safe_same  = [cb for cb in safe if cb[0] == cb[1]]
        safe_diff  = [cb for cb in safe if cb[0] != cb[1]]

        if safe_same:
            print("  【重复字符】更易记忆：")
            row = '  '
            for cb in safe_same:
                row += f"  {cb.decode('latin-1'):<5}"
            print(row)

        if safe_diff:
            print()
            print("  【两字符组合】：")
            # 每行打印 10 个
            for i in range(0, len(safe_diff), 10):
                row = '  '
                for cb in safe_diff[i:i+10]:
                    row += f"  {cb.decode('latin-1'):<5}"
                print(row)

        # 推荐最佳
        best = safe_same[0] if safe_same else safe_diff[0]
        best_str = best.decode('latin-1')
        print()
        print(f"  ★ 最优推荐：'{best_str}'")
        print()
        print("  使用示例（Python）：")
        print(f"    SEP = {repr(best_str)}")
        print(f"    fields = line.rstrip('\\n').split(SEP)")
        print()
        print("  使用示例（csv 模块，需自定义 dialect）：")
        print(f"    import csv")
        print(f"    # csv 模块只支持单字符分隔符，双字符需用 str.split()")
        print(f"    # 推荐直接用：line.split({repr(best_str)})")
    else:
        print("  所有组合均已存在于文件中")

    # ── 低频组合（备选）──────────────────────────────────────────────────────
    print()
    print(sep)
    print(f"  ⚠️  文件中存在的组合（出现次数最少前 10 个，备选参考）")
    print(sep)
    for cb, cnt in unsafe[:10]:
        s = cb.decode('latin-1')
        print(f"  '{s}'   出现 {cnt:>14,} 次{note}")

    print()
    if is_sample:
        print(f"  提示：当前仅扫描前 {scan_size//1024//1024} MB，"
              f"加 --full 可全文件确认结果")
    print()


if __name__ == '__main__':
    main()
