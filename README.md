# DB Importer

面向大文件场景的数据导入 / 导出工具，提供图形界面（GUI）和命令行工具（CLI）。

支持将 CSV、固定宽度等格式的文本文件导入 **SQLite / MySQL / Oracle** 数据库，也可将数据导出为 **CSV / Excel**。

---

## 功能特性

### 图形界面（csv_importer.py）

| 模块 | 功能 |
|------|------|
| 文件配置 | 自动检测编码、分隔符；支持分隔符格式与固定宽度格式 |
| 数据预览 | 分页浏览，奇偶列交替着色，双击表头可重命名列 |
| 数据校验 | 流式单遍扫描，不将全文件载入内存；实时显示已校验行数 |
| 数据导入 | 流式读取，逐批写库；支持跳过头 / 尾 / 中间行 |
| 数据导出 | 流式输出 CSV / Excel；支持按行数分片；记录导出日志 |
| 多数据库 | SQLite（内置）/ MySQL / Oracle Thick & Thin 模式 |

### 命令行工具

| 脚本 | 用途 |
|------|------|
| `peek_file.py` | 快速查看大文件头尾各 N 行，自动检测编码 |
| `count_lines.py` | 高效统计行数（二进制分块，不全量加载） |
| `split_file.py` | 按行数或按大小拆分大文件，不在行中间截断 |

---

## 环境要求

- Python 3.9+
- Windows 10/11（GUI 依赖 tkinter，已随 Python 内置）

---

## 安装

```bash
git clone https://github.com/your-username/DB_importer.git
cd DB_importer

pip install -r requirements.txt
```

按需安装数据库驱动：

```bash
# MySQL
pip install mysql-connector-python

# Oracle（Instant Client 12.2 兼容版）
pip install "oracledb<2.0" cryptography
```

---

## 使用方法

### 图形界面

```bash
python csv_importer.py
```

**基本流程：**

1. **文件配置** — 选择文件，确认编码、分隔符（或固定宽度切割位置）
2. **数据预览** — 浏览数据，双击表头重命名列名
3. **数据校验** — 检测字段数异常行，支持将异常行加入忽略列表
4. **数据导入** — 选择目标数据库和表，流式导入
5. **数据导出** — 选择导出格式（CSV / Excel）和输出目录

### 命令行工具

```bash
# 查看文件头尾各 100 行
python peek_file.py data.csv 100

# 统计行数
python count_lines.py data.csv

# 按行数拆分（每 100 万行一个文件）
python split_file.py data.csv --lines 1000000

# 按大小拆分（每 500 MB 一个文件）
python split_file.py data.csv --size-mb 500 --out-dir ./parts
```

---

## 大文件处理说明

本工具针对 **10GB+、千万行级** 文件做了专项优化：

- **流式校验**：单遍扫描，仅记录异常行，不将全部数据载入内存
- **流式导入 / 导出**：每次只读取一批数据，内存占用恒定
- **Excel 流式写入**：xlsxwriter `constant_memory=True` + `use_zip64=True`，支持超过 4 GB 的 xlsx 文件
- **编码检测**：优先使用 `chardet`，失败时依次尝试 `utf-8 / gbk / latin-1`

---

## 打包为 EXE（Windows）

```bash
build_exe.bat
```

产物位于 `dist\CSV导入工具.exe`，单文件无需安装 Python。

使用 Oracle 时，需将 `instantclient_xx_x\` 目录放在 exe 同目录下。

---

## 目录结构

```
DB_importer/
├── csv_importer.py        # 主程序（GUI）
├── file_utils.py          # 文件工具库（被主程序和 CLI 共享）
├── peek_file.py           # CLI：查看文件头尾
├── count_lines.py         # CLI：统计行数
├── split_file.py          # CLI：拆分大文件
├── requirements.txt       # Python 依赖
├── build_exe.bat          # 打包脚本（Windows）
├── CSV导入工具.spec        # PyInstaller 配置
└── logs/                  # 运行日志（自动生成，不纳入版本控制）
```

运行时自动生成（已在 `.gitignore` 中排除）：

```
db_config.json    # 数据库连接配置（含账号密码，不要提交）
ui_state.json     # UI 状态（上次打开的文件路径等）
logs/             # 导入 / 导出日志
```

---

## 数据库支持

| 数据库 | 驱动 | 说明 |
|--------|------|------|
| SQLite | 内置 | 无需额外安装 |
| MySQL | mysql-connector-python | 按需安装 |
| Oracle | oracledb | 支持 Thin（纯 Python）和 Thick（需 Instant Client）模式 |

---

## License

MIT
