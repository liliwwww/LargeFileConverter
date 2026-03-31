# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all, collect_submodules

datas     = []
binaries  = []
hiddenimports = []

# ── cryptography（oracledb Thin 模式必须）────────────────────────────────────
tmp = collect_all('cryptography')
datas += tmp[0]; binaries += tmp[1]; hiddenimports += tmp[2]

# ── 翻译文件：locales/ 目录打包进 EXE ────────────────────────────────────────
datas += [('locales', 'locales')]

# ── 数据库驱动（按需，未安装时对应功能不可用，不会崩溃）─────────────────────
for pkg in ('mysql.connector', 'oracledb'):
    try:
        tmp = collect_all(pkg)
        datas += tmp[0]; binaries += tmp[1]; hiddenimports += tmp[2]
    except Exception:
        pass

# ── Excel 引擎 ────────────────────────────────────────────────────────────────
for pkg in ('xlsxwriter', 'openpyxl'):
    try:
        hiddenimports += collect_submodules(pkg)
    except Exception:
        pass

hiddenimports += [
    'chardet',
    'cryptography',
    'cryptography.hazmat.primitives.ciphers.algorithms',
    'cryptography.hazmat.primitives.ciphers.modes',
    'cryptography.hazmat.backends.openssl',
]

a = Analysis(
    ['csv_importer.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='DB_Importer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
