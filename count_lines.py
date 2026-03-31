#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统计文件行数（高效版，支持大文件）
用法: python count_lines.py <文件路径>
"""

import sys
import os
from file_utils import count_lines


def main():
    if len(sys.argv) < 2:
        print("用法: python count_lines.py <文件路径>")
        sys.exit(1)

    path = sys.argv[1]
    if not os.path.exists(path):
        print(f"错误：文件不存在: {path}")
        sys.exit(1)

    size  = os.path.getsize(path)
    lines = count_lines(path)
    print(f"文件: {path}")
    print(f"大小: {size:,} 字节  ({size / 1024 / 1024:.2f} MB)")
    print(f"行数: {lines:,}")


if __name__ == "__main__":
    main()
