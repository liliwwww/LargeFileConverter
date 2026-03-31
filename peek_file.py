#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大文件头尾探测工具
用法: python peek_file.py <文件路径> [行数=100]
"""

import sys
import os
from file_utils import detect_encoding, read_head, read_tail


def peek(path: str, n: int = 100):
    if not os.path.exists(path):
        print(f"文件不存在: {path}")
        sys.exit(1)

    file_size = os.path.getsize(path)
    encoding  = detect_encoding(path)

    print(f"文件路径 : {path}")
    print(f"文件大小 : {file_size:,} 字节 ({file_size / 1024**3:.2f} GB)")
    print(f"检测编码 : {encoding}")
    print(f"提取行数 : 前/后各 {n} 行")
    print()

    head = read_head(path, encoding, n)
    print(f"{'─'*20} 前 {n} 行 {'─'*20}")
    for i, line in enumerate(head, 1):
        print(f"{i:>6}: {line[:200]}")

    tail = read_tail(path, n, encoding=encoding)
    print()
    print(f"{'─'*20} 后 {n} 行 {'─'*20}")
    for i, line in enumerate(tail, 1):
        print(f"{i:>6}: {line[:200]}")

    out_path = path + '.peek.txt'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"文件: {path}\n大小: {file_size:,} 字节\n编码: {encoding}\n\n")
        f.write(f"{'─'*40}\n前 {n} 行\n{'─'*40}\n")
        for i, line in enumerate(head, 1):
            f.write(f"{i:>6}: {line}\n")
        f.write(f"\n{'─'*40}\n后 {n} 行\n{'─'*40}\n")
        for i, line in enumerate(tail, 1):
            f.write(f"{i:>6}: {line}\n")

    print(f"\n结果已保存到: {out_path}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    n = int(sys.argv[2]) if len(sys.argv) >= 3 else 100
    peek(sys.argv[1], n)
