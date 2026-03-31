#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大文件拆分工具

按行数拆分:
  python split_file.py <文件> --lines 1000000

按大小拆分 (到达目标大小后在行尾切断):
  python split_file.py <文件> --size-mb 500

可选参数:
  --out-dir <目录>   输出目录，默认与原文件同目录
  --prefix  <前缀>   输出文件名前缀，默认为原文件名（无扩展名）
  --no-progress      不显示进度
"""

import os
import sys
import time
import argparse
from file_utils import split_file


def _make_progress_cb(file_size: int, show: bool):
    """返回一个进度回调，show=False 时返回 None。"""
    if not show:
        return None
    t0 = time.time()
    def _cb(done: int, total: int):
        elapsed = time.time() - t0 or 1e-9
        pct   = done / total * 100 if total else 0
        speed = done / elapsed / 1024 / 1024
        eta   = (total - done) / (done / elapsed) if done > 0 else 0
        print(
            f"\r  {pct:5.1f}%  {speed:6.1f} MB/s  "
            f"{done/1024/1024:,.0f}/{total/1024/1024:,.0f} MB  "
            f"剩余 {eta:.0f}s   ",
            end="", flush=True,
        )
    return _cb


def main():
    parser = argparse.ArgumentParser(
        description="大文件按行拆分工具（不在行中间切断）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("filepath", help="要拆分的文件路径")
    parser.add_argument("--lines",   type=int,   default=0,
                        help="每个子文件的行数（与 --size-mb 二选一）")
    parser.add_argument("--size-mb", type=float, default=0,
                        help="每个子文件的目标大小（MB），到达后在行尾切断")
    parser.add_argument("--out-dir", default="",
                        help="输出目录，默认与原文件同目录")
    parser.add_argument("--prefix",  default="",
                        help="输出文件名前缀，默认使用原文件名（无扩展名）")
    parser.add_argument("--no-progress", action="store_true",
                        help="不显示进度条")
    args = parser.parse_args()

    src = args.filepath
    if not os.path.exists(src):
        print(f"错误：文件不存在: {src}")
        sys.exit(1)

    if args.lines == 0 and args.size_mb == 0:
        parser.error("请指定 --lines 或 --size-mb 之一")
    if args.lines > 0 and args.size_mb > 0:
        parser.error("--lines 和 --size-mb 不能同时使用")

    file_size = os.path.getsize(src)
    base      = os.path.basename(src)
    name, ext = os.path.splitext(base)
    ext       = ext or ".txt"

    out_dir = args.out_dir or os.path.dirname(os.path.abspath(src))
    os.makedirs(out_dir, exist_ok=True)

    prefix = args.prefix or name
    show   = not args.no_progress

    print()
    print(f"源文件  : {src}")
    print(f"大小    : {file_size:,} 字节  ({file_size/1024**3:.2f} GB)")
    print(f"输出目录: {out_dir}")
    if args.lines:
        print(f"拆分方式: 每 {args.lines:,} 行一个文件")
    else:
        print(f"拆分方式: 每 {args.size_mb:g} MB 一个文件（在行尾切断）")
    print()

    t0 = time.time()
    progress_cb = _make_progress_cb(file_size, show)

    try:
        results = split_file(
            src, out_dir, prefix, ext,
            lines_per_file=args.lines,
            max_bytes=int(args.size_mb * 1024 * 1024) if args.size_mb else 0,
            progress_cb=progress_cb,
        )
    except FileExistsError as e:
        print(f"\n错误：{e}")
        sys.exit(1)

    elapsed = time.time() - t0
    speed   = file_size / elapsed / 1024 / 1024
    print(f"\n完成！共 {len(results)} 个文件，耗时 {elapsed:.1f}s，"
          f"平均速度 {speed:.1f} MB/s\n")
    for fpath, lc, bc in results:
        print(f"  {fpath}  （{lc:,} 行, {bc/1024/1024:.2f} MB）")
    print()


if __name__ == "__main__":
    main()
