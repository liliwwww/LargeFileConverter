#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
file_utils.py — 大文件通用工具库

提供以下函数，供 csv_importer.py / peek_file.py /
split_file.py / count_lines.py 共同调用：

  detect_encoding(path)             → str
  count_lines(path)                 → int
  read_head(path, encoding, n)      → list[str]
  read_tail(path, n, encoding, ...) → list[str]
  read_line_at(path, encoding, idx) → str | None   （第 idx 行，0-based）
  read_last_nth_line(path, n, enc)  → str | None   （倒数第 n 行，1=最后一行）
  split_file(src, out_dir, prefix, ext,
             lines_per_file=0, max_bytes=0,
             progress_cb=None)      → list[tuple[str,int,int]]
"""

import os

_READ_BUF = 8 * 1024 * 1024   # 8 MB


# ── 编码检测 ──────────────────────────────────────────────────────────────────

def detect_encoding(path: str) -> str:
    """
    自动检测文件编码。
    优先使用 chardet（需安装），fallback 逐一尝试常见编码。
    """
    try:
        import chardet
        with open(path, 'rb') as f:
            raw = f.read(200_000)
        enc = chardet.detect(raw).get('encoding') or 'utf-8'
        mapping = {'GB2312': 'gbk', 'ascii': 'utf-8', 'ISO-8859-1': 'latin-1'}
        return mapping.get(enc, enc)
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


# ── 统计行数 ──────────────────────────────────────────────────────────────────

def count_lines(path: str) -> int:
    """
    二进制分块统计 \\n 数量，不加载全文件到内存。
    末尾无换行符时最后一行也计入。
    """
    count = 0
    with open(path, 'rb') as f:
        while True:
            buf = f.read(_READ_BUF)
            if not buf:
                break
            count += buf.count(b'\n')

    size = os.path.getsize(path)
    if size > 0:
        with open(path, 'rb') as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b'\n':
                count += 1
    return count


# ── 读头部 n 行 ───────────────────────────────────────────────────────────────

def read_head(path: str, encoding: str, n: int) -> list:
    """
    顺序读取前 n 行，读完即停，不加载全文件。
    返回去掉行尾换行的字符串列表。
    """
    lines = []
    with open(path, encoding=encoding, errors='replace') as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            lines.append(line.rstrip('\r\n'))
    return lines


# ── 读尾部 n 行 ───────────────────────────────────────────────────────────────

def read_tail(path: str, n: int, encoding: str = 'utf-8',
              chunk_size: int = 1024 * 1024) -> list:
    """
    二进制反向分块读取，找到最后 n 行后解码返回。
    不加载全文件，适合 20 GB 以上大文件。
    encoding 须与文件真实编码一致，否则多字节字符会乱码。
    """
    buf = b''
    pos = os.path.getsize(path)
    with open(path, 'rb') as f:
        while pos > 0 and buf.count(b'\n') < n + 1:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            buf = f.read(read_size) + buf

    text = buf.decode(encoding, errors='replace')
    lines = text.splitlines()
    while lines and lines[-1].strip() == '':
        lines.pop()
    return lines[-n:]


# ── 读任意行（按行号，0-based） ────────────────────────────────────────────────

def read_line_at(path: str, encoding: str, idx: int):
    """
    顺序读到第 idx 行（0-based）即停，返回该行字符串（去行尾换行）。
    找不到（文件行数不足）返回 None。
    适合"读取忽略前 h 行后的第一行"场景（idx = h）。
    """
    with open(path, encoding=encoding, errors='replace') as f:
        for i, line in enumerate(f):
            if i == idx:
                return line.rstrip('\r\n')
    return None


# ── 读倒数第 n 行（1=最后一行） ───────────────────────────────────────────────

def read_last_nth_line(path: str, n: int, encoding: str = 'utf-8',
                       chunk_size: int = 512 * 1024):
    """
    反向读取，返回倒数第 n 行（去行尾换行）。
    n=1 → 最后一行；n=2 → 倒数第二行，以此类推。
    找不到返回 None。
    """
    buf = b''
    pos = os.path.getsize(path)
    needed = n + 1
    with open(path, 'rb') as f:
        while pos > 0 and buf.count(b'\n') < needed:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            buf = f.read(read_size) + buf

    lines = buf.decode(encoding, errors='replace').splitlines()
    while lines and lines[-1].strip() == '':
        lines.pop()
    if len(lines) >= n:
        return lines[-n]
    return None


# ── 文件拆分 ──────────────────────────────────────────────────────────────────

def split_file(src: str, out_dir: str, prefix: str, ext: str,
               lines_per_file: int = 0, max_bytes: int = 0,
               progress_cb=None) -> list:
    """
    将 src 拆分为多个小文件，只在 \\n 处切断（不破坏行完整性）。

    参数：
      lines_per_file  每个子文件最多行数（与 max_bytes 二选一，不能同时非0）
      max_bytes       每个子文件目标字节数，达到后在行尾切断
      progress_cb     可选进度回调 progress_cb(done_bytes: int, total_bytes: int)
                      传 None 则不回调

    返回：
      list of (file_path, line_count, byte_count)

    异常：
      FileExistsError  若任意输出文件已存在则立即抛出，不覆盖文件
      ValueError       lines_per_file 和 max_bytes 都为 0 或都非 0
    """
    if (lines_per_file == 0) == (max_bytes == 0):
        raise ValueError("lines_per_file 和 max_bytes 须恰好一个非零")

    file_size  = os.path.getsize(src)
    results    = []
    file_idx   = 0
    cur_lines  = 0
    cur_bytes  = 0
    out_fh     = None
    cur_path   = None
    done_bytes = 0

    def _open_next():
        nonlocal file_idx, out_fh, cur_path, cur_lines, cur_bytes
        if out_fh:
            out_fh.close()
            results.append((cur_path, cur_lines, cur_bytes))
        file_idx += 1
        cur_lines = 0
        cur_bytes = 0
        cur_path  = os.path.join(out_dir, f"{prefix}_split_{file_idx:04d}{ext}")
        if os.path.exists(cur_path):
            raise FileExistsError(
                f"输出文件已存在，拆分终止（不覆盖任何文件）：\n{cur_path}"
            )
        out_fh = open(cur_path, 'wb')

    _open_next()

    try:
        with open(src, 'rb') as fh:
            while True:
                buf = fh.read(_READ_BUF)
                if not buf:
                    break
                done_bytes += len(buf)
                if progress_cb:
                    progress_cb(done_bytes, file_size)

                start = 0
                while True:
                    nl = buf.find(b'\n', start)
                    if nl == -1:
                        fragment = buf[start:]
                        out_fh.write(fragment)
                        cur_bytes += len(fragment)
                        break
                    line = buf[start:nl + 1]
                    out_fh.write(line)
                    cur_bytes += len(line)
                    cur_lines += 1
                    start = nl + 1

                    cut = (lines_per_file and cur_lines >= lines_per_file) or \
                          (max_bytes      and cur_bytes >= max_bytes)
                    if cut:
                        _open_next()
    except Exception:
        if out_fh:
            out_fh.close()
        raise

    # 最后一个文件
    if out_fh:
        out_fh.close()
        if cur_lines > 0 or cur_bytes > 0:
            results.append((cur_path, cur_lines, cur_bytes))
        else:
            os.remove(cur_path)   # 空文件删掉

    return results
