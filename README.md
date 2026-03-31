# DB Importer

专为**大文件**设计的数据导入 / 导出工具，提供图形界面（GUI）和命令行工具（CLI）。

无需将大文件用文本编辑器打开（大文件会导致编辑器卡死或崩溃）——DB Importer 全程流式处理，内存占用恒定，轻松应对 **10 GB+、千万行级** 数据文件。

---

## 核心特性

### 大文件安全探测

在导入前充分了解文件内容，避免盲目操作：

| 工具 | 说明 |
|------|------|
| **行数统计** | 二进制分块扫描，秒级统计亿行文件行数，不全量加载 |
| **头尾预览** | 查看文件前后各 N 行，自动检测编码，结果保存为 `.peek.txt` |
| **大文件拆分** | 按行数或按文件大小拆分，严格在行尾截断，不破坏数据完整性 |

### 灵活的文件格式支持

- **分隔符格式**：自动检测编码和分隔符，支持任意单字符分隔符
- **固定宽度格式**：手动指定列起始位置，或使用内置**智能探测**功能自动推断分割规则
- **忽略行配置**：可跳过文件头部 N 行、尾部 N 行，以及任意中间行范围（如 `100-200,500`）

### 数据预览与校验

- 分页数据预览，**奇偶列交替着色**，大量列也能清晰辨识
- **双击表头**可直接重命名列名，自定义列名贯穿后续校验、导入、导出全流程
- **流式校验**：单遍扫描，不将数据载入内存，实时显示已校验行数，支持亿级行文件
- 校验异常行（字段数不符）可**一键加入忽略行配置**，自动排除脏数据
- 导入前提供**信息摘要**：总行数、有效数据行数、忽略行数、异常行数，所见即所得

### 多数据库支持

| 数据库 | 特点 |
|--------|------|
| **SQLite** | 内置支持，零安装，开箱即用，适合本地数据处理 |
| **MySQL** | 需安装 `mysql-connector-python` |
| **Oracle** | 支持 Thin 模式（纯 Python）和 Thick 模式（需 Instant Client），兼容 12.2+ |

### 灵活导出

- **数据源**：从已校验的 CSV 文件直接导出，或通过**自定义 SQL 语句**查询后导出
- **格式**：CSV（符合 RFC 4180 标准，字段含逗号自动加引号）或 Excel（`.xlsx`）
- **Excel 引擎**：支持 `xlsxwriter`（速度快）和 `openpyxl`（兼容性好）两种引擎
- **分片导出**：按指定行数拆分为多个文件，防止单文件过大无法在 Excel 中打开
- **自定义表头**：导入和导出均支持自定义列名，独立于源文件原始表头
- **导出日志**：每次导出自动生成 `logs/export_YYYYMMDD_HHMMSS.log`，记录分阶段耗时

---

## 环境要求

- Python 3.9+
- Windows 10 / 11（GUI 依赖 tkinter，已随 Python 内置）

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

**标准工作流：**

```
文件配置  →  数据预览  →  数据校验  →  数据导入 / 数据导出
```

1. **文件配置** — 选择文件，确认编码、分隔符格式或固定宽度切割位置；配置忽略行
2. **数据预览** — 分页浏览数据，双击表头重命名列名
3. **数据校验** — 流式扫描，查看异常行，一键将异常行加入忽略配置
4. **数据导入** — 选择目标数据库和表，确认导入摘要，流式写入
5. **数据导出** — 选择数据源（文件 / SQL）、格式、分片规则，流式输出

### 命令行工具

```bash
# 查看文件头尾各 100 行（自动检测编码，结果保存到 data.csv.peek.txt）
python peek_file.py data.csv 100

# 统计行数
python count_lines.py data.csv

# 按行数拆分（每 100 万行一个文件）
python split_file.py data.csv --lines 1000000

# 按大小拆分（每 500 MB 一个文件，输出到 ./parts/）
python split_file.py data.csv --size-mb 500 --out-dir ./parts
```

---

## 大文件处理原理

| 环节 | 策略 |
|------|------|
| 校验 | 单遍流式扫描，仅记录异常行行号，内存占用与文件大小无关 |
| 导入 | 流式重读文件，逐批写库，批大小可配置 |
| 导出（CSV） | 流式写入，边读边写 |
| 导出（Excel） | xlsxwriter `constant_memory=True` + `use_zip64=True`，支持超 4 GB xlsx |
| 文件拆分 | 二进制分块读取，严格在 `\n` 处截断，不破坏行完整性 |
| 行数统计 | 二进制分块统计 `\n`，8 MB 缓冲区，不全量加载 |
| 头尾预览 | 反向二进制分块读取尾部，不扫描全文件 |

---

## 打包为 EXE（Windows）

```bash
build_exe.bat
```

产物位于 `dist\CSV导入工具.exe`，单文件，无需安装 Python。

使用 Oracle Thick 模式时，需将 `instantclient_xx_x\` 目录放在 exe 同目录下，程序启动时自动检测。

---

## 目录结构

```
DB_importer/
├── csv_importer.py        # 主程序（GUI）
├── file_utils.py          # 文件工具库（被主程序和 CLI 共同依赖）
├── peek_file.py           # CLI：查看文件头尾
├── count_lines.py         # CLI：统计行数
├── split_file.py          # CLI：拆分大文件
├── requirements.txt       # Python 依赖
├── build_exe.bat          # 打包脚本（Windows）
├── CSV导入工具.spec        # PyInstaller 配置
├── README.md
└── LICENSE
```

运行时自动生成（已在 `.gitignore` 中排除，请勿提交）：

```
db_config.json    # 数据库连接配置（含账号密码）
ui_state.json     # UI 状态（上次打开的文件路径等）
logs/             # 导入 / 导出运行日志
```

---

## 依赖说明

```
chardet          # 文件编码自动检测（必装）
xlsxwriter       # Excel 导出，流式写入（推荐）
openpyxl         # Excel 导出，备选引擎
```

数据库驱动按需安装，不安装对应驱动时该数据库类型在界面中不可选，其余功能不受影响。

---

## License

MIT
