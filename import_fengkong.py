import sqlite3
import sys
import os

CSV_FILE = "DBC0000028580/fengkong.csv"
DB_FILE = "abc.db"
TABLE_NAME = "fengkong"
DELIMITER = "|"
EXPECTED_DELIMITERS = 6  # 6个|分隔符，即7个字段


def main():
    if not os.path.exists(CSV_FILE):
        print(f"错误：找不到文件 {CSV_FILE}")
        sys.exit(1)

    # 自动检测编码：优先尝试 utf-8-sig（带BOM的UTF-8），再尝试 gbk
    for encoding in ("utf-8-sig", "utf-8", "gbk", "gb2312"):
        try:
            with open(CSV_FILE, "r", encoding=encoding) as f:
                lines = f.readlines()
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        print("错误：无法识别文件编码，请手动指定编码")
        sys.exit(1)

    if not lines:
        print("错误：CSV文件为空")
        sys.exit(1)

    # 验证所有行的分隔符数量，收集所有无效数据
    invalid_lines = []
    for i, line in enumerate(lines, start=1):
        line_stripped = line.rstrip("\n").rstrip("\r")
        count = line_stripped.count(DELIMITER)
        if count != EXPECTED_DELIMITERS:
            invalid_lines.append((i, count, line_stripped))

    if invalid_lines:
        print(f"发现 {len(invalid_lines)} 条无效数据：")
        for row_num, count, content in invalid_lines:
            print(f"  第 {row_num} 行：分隔符数量 {count} 个，内容：{content}")
        sys.exit(1)

    # 解析表头（第一行）
    header = lines[0].rstrip("\n").rstrip("\r").split(DELIMITER)
    columns = [col.strip() for col in header]
    col_count = len(columns)

    # 创建 SQLite 数据库和表
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    col_definitions = ", ".join([f'"{col}" TEXT' for col in columns])
    cursor.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
    cursor.execute(f'CREATE TABLE "{TABLE_NAME}" ({col_definitions})')

    # 插入数据行（跳过表头）
    placeholders = ", ".join(["?" for _ in columns])
    insert_sql = f'INSERT INTO "{TABLE_NAME}" VALUES ({placeholders})'

    rows = []
    for line in lines[1:]:
        line_stripped = line.rstrip("\n").rstrip("\r")
        values = [v.strip() for v in line_stripped.split(DELIMITER)]
        rows.append(values)

    cursor.executemany(insert_sql, rows)
    conn.commit()
    conn.close()

    print(f"导入成功：共导入 {len(rows)} 条数据到 {DB_FILE} 的 {TABLE_NAME} 表中")


if __name__ == "__main__":
    main()
