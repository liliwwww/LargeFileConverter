# LargeFileConverter

A data import/export tool designed specifically for **large files**, providing both a graphical user interface (GUI) and command-line interface (CLI).

No need to open large files in a text editor (which can cause the editor to freeze or crash). LargeFileConverter performs fully streaming processing with constant memory usage, easily handling **10 GB+ files containing tens of millions of rows**.

---

## Core Features

### Safe Probing for Large Files

Fully understand the file contents before importing to avoid blind operations:

| Tool                  | Description |
|-----------------------|-------------|
| **Line Count**        | Binary chunk scanning; counts lines in billion-row files in seconds without full loading |
| **Head/Tail Preview** | Displays the first and last N lines of the file, automatically detects encoding, and saves results to `.peek.txt` |
| **Large File Splitting** | Split by number of lines or file size; strictly truncates at line endings to preserve data integrity |

### Flexible File Format Support

- **Delimiter Format**: Automatically detects encoding and delimiter; supports any single-character delimiter
- **Fixed-Width Format**: Manually specify column start positions, or use the built-in **smart detection** feature to automatically infer splitting rules
- **Ignore Row Configuration**: Skip the first N lines, the last N lines, and any intermediate row ranges (e.g., `100-200,500`)

### Data Preview and Validation

- Paged data preview with **alternating odd/even column coloring** — even files with many columns remain easy to read
- **Double-click any column header** to rename it instantly; custom column names persist throughout validation, import, and export
- **Streaming Validation**: Single-pass scan with no data loaded into memory; shows validated row count in real time and supports billion-row files
- Exception rows (mismatched field count) can be **added to the ignore-row configuration with one click**, automatically excluding dirty data
- Pre-import **summary report**: total rows, valid data rows, ignored rows, and exception rows — what you see is what you get

### Multi-Database Support

| Database   | Features |
|------------|----------|
| **SQLite** | Built-in support, zero installation, ready to use — perfect for local data processing |
| **MySQL**  | Requires `mysql-connector-python` |
| **Oracle** | Supports Thin mode (pure Python) and Thick mode (requires Instant Client); compatible with 12.2+ |

### Flexible Export

- **Data Source**: Export directly from a validated CSV file, or run a **custom SQL query** first
- **Format**: CSV (RFC 4180 compliant — fields containing commas are automatically quoted) or Excel (`.xlsx`)
- **Excel Engine**: Supports `xlsxwriter` (fast) and `openpyxl` (better compatibility)
- **Sharded Export**: Split output into multiple files by a specified row count to prevent files that are too large to open in Excel
- **Custom Headers**: Both import and export support custom column names, independent of the original file headers
- **Export Logs**: Every export automatically creates `logs/export_YYYYMMDD_HHMMSS.log`, recording timing for each processing stage

---

## Environment Requirements

- Python 3.9+
- Windows 10 / 11 (GUI relies on tkinter, which is included with Python)

---

## Installation

```bash
git clone https://github.com/your-username/LargeFileConverter.git
cd LargeFileConverter

pip install -r requirements.txt
```

---

## Usage

### Graphical User Interface


```bash
python csv_importer.py
```

### Standard Workflow:

```bash
File Configuration → Data Preview → Data Validation → Data Import / Data Export
```
- **1.File Configuration** — Select the file, confirm encoding, delimiter (or fixed-width column positions), and configure rows to ignore  
- **2.Data Preview** — Browse data page by page and double-click column headers to rename them  
- **3.Data Validation** — Streaming scan; review exception rows and add them to the ignore list with one click  
- **4.Data Import** — Choose target database and table, review the import summary, then stream-write the data  
- **5.Data Export** — Select data source (file or SQL), output format, and sharding rules, then stream the output

---

## Command-Line Tools

```bash
# View first and last 100 lines (auto-detects encoding; results saved to data.csv.peek.txt)
python peek_file.py data.csv 100

# Count total lines
python count_lines.py data.csv

# Split by line count (1 million lines per file)
python split_file.py data.csv --lines 1000000

# Split by file size (500 MB per file, output to ./parts/)
python split_file.py data.csv --size-mb 500 --out-dir ./parts
```

---

## Large File Processing Principles

| Stage| Strategy |
|------|------|
|Validation|Single-pass streaming scan; only records exception row numbers — memory usage independent of file size|
|Import|Re-reads file in streaming mode and writes to database in configurable batches|
|Export (CSV)|Fully streaming read-and-write|
|Export (Excel)|Uses xlsxwriter with `constant_memory=True` + `use_zip64=True`, supporting .xlsx |files larger than 4 GB
|File Splitting|Binary chunk reading; strictly truncates at `\n` to preserve complete rows|
|Line Counting|Binary chunk counting of `\n` with 8 MB buffer — no full file loading|
|Head/Tail Preview|Reverse binary chunk reading for the tail; does not scan the entire file|


---

## Packaging as EXE (Windows)

```bash
build_exe.bat
```
The executable is placed in the `dist\` folder as a single ZIP file. Unzip and run — no Python installation required.

When using Oracle Thick mode, place the `instantclient_xx_x\` folder in the same directory as the EXE; the program will detect it automatically on startup.


> **Oracle Users:** Due to Oracle licensing restrictions, Instant Client is not bundled with the program. Download it yourself from the official Oracle website:


> https://www.oracle.com/database/technologies/instant-client/downloads.html

---

## Project Structure

```
LargeFileConverter/
├── csv_importer.py          # Main GUI program
├── file_utils.py            # Shared file utilities (used by GUI and CLI)
├── peek_file.py             # CLI: View file head/tail
├── count_lines.py           # CLI: Count total lines
├── split_file.py            # CLI: Split large files
├── requirements.txt         # Python dependencies
├── build_exe.bat            # Windows packaging script
├── CSV Import Tool.spec     # PyInstaller configuration
├── README.md
└── LICENSE
```

Files automatically generated at runtime (already excluded via `.gitignore` — do not commit them):



```
db_config.json      # Database connection settings (includes credentials)
ui_state.json       # UI state (last opened file path, etc.)
logs/               # Import/export runtime logs
```

---

## Dependencies

```
chardet          # Automatic file encoding detection (required)
xlsxwriter       # Fast streaming Excel export (recommended)
openpyxl         # Alternative Excel export engine
```

Database drivers are optional. If a driver is not installed, its corresponding database option is disabled in the GUI; all other features remain fully functional.




---

## License



