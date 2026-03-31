import sys
import os

CSV_FILE = "DBC0000028580/fengkong.csv"
DELIMITER = "|"
EXPECTED_DELIMITERS = 6

# 重点检查范围：卡住行附近
CHECK_AROUND = 1090000
WINDOW = 100  # 前后各100行


def main():
    if not os.path.exists(CSV_FILE):
        print(f"错误：找不到文件 {CSV_FILE}")
        sys.exit(1)

    for encoding in ("utf-8-sig", "utf-8", "gbk", "gb2312"):
        try:
            with open(CSV_FILE, "r", encoding=encoding) as f:
                f.read(1024)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        print("错误：无法识别文件编码")
        sys.exit(1)

    print(f"使用编码：{encoding}")
    print(f"扫描范围：第 {CHECK_AROUND - WINDOW} ~ {CHECK_AROUND + WINDOW} 行\n")

    invalid_count = 0
    with open(CSV_FILE, "r", encoding=encoding) as f:
        for i, line in enumerate(f, start=1):
            # 只检查目标范围附近
            if i < CHECK_AROUND - WINDOW:
                continue
            if i > CHECK_AROUND + WINDOW:
                break

            line_stripped = line.rstrip("\r\n")
            count = line_stripped.count(DELIMITER)

            # 检查空行
            if not line_stripped:
                print(f"  第 {i} 行：空行")
                invalid_count += 1
                continue

            # 检查分隔符数量
            if count != EXPECTED_DELIMITERS:
                print(f"  第 {i} 行：分隔符 {count} 个（期望 {EXPECTED_DELIMITERS}），内容：{line_stripped[:200]}")
                invalid_count += 1
                continue

            # 检查是否含有不可见控制字符（常见导致卡死的原因）
            has_ctrl = any(ord(c) < 32 and c not in ('\t',) for c in line_stripped)
            if has_ctrl:
                clean = "".join(c if ord(c) >= 32 or c == '\t' else f"[0x{ord(c):02X}]" for c in line_stripped)
                print(f"  第 {i} 行：含控制字符，内容：{clean[:200]}")
                invalid_count += 1

    if invalid_count == 0:
        print("该范围内未发现异常，问题可能在事务提交或内存上，建议缩小 DBeaver 的 commit size")
    else:
        print(f"\n共发现 {invalid_count} 条异常数据")


if __name__ == "__main__":
    main()
