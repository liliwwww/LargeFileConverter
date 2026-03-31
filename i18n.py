#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
i18n.py — 轻量国际化模块

用法：
    from i18n import _, set_lang, current_lang, available_langs

    set_lang("en_US")          # 切换语言（重新创建窗口后生效）
    label = _("export.browse") # 取翻译文本
    msg   = _("export.col_info_valid", valid=1000, total=1050, inv=50)  # 带变量
"""

import json
import os
import sys

_translations: dict = {}
_current_lang: str  = "zh_CN"

# PyInstaller 打包后 __file__ 在 sys._MEIPASS 临时目录里；
# 直接运行时 __file__ 是脚本所在目录。两种情况都正确指向 locales/。
if getattr(sys, "frozen", False):
    _BASE_DIR = sys._MEIPASS
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_LOCALES_DIR: str = os.path.join(_BASE_DIR, "locales")

LANG_LABELS = {
    "zh_CN": "中文",
    "en_US": "English",
    "ja_JP": "日本語",
}


def available_langs() -> list:
    """返回 locales/ 下已有翻译的语言代码列表，保持固定顺序。"""
    order = list(LANG_LABELS.keys())
    found = []
    for lang in order:
        if os.path.exists(os.path.join(_LOCALES_DIR, f"{lang}.json")):
            found.append(lang)
    return found


def current_lang() -> str:
    return _current_lang


def set_lang(lang: str) -> None:
    global _translations, _current_lang
    path = os.path.join(_LOCALES_DIR, f"{lang}.json")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        _translations = json.load(f)
    _current_lang = lang


def _(key: str, **kwargs) -> str:
    """
    返回 key 对应的翻译文本。
    - 找不到 key 时直接返回 key 本身（方便调试：屏幕上会显示 key 名）。
    - 支持 str.format_map 插值，例如 _("export.progress", done=100, total=200)。
    """
    text = _translations.get(key, key)
    if kwargs:
        try:
            text = text.format_map(kwargs)
        except (KeyError, ValueError):
            pass
    return text


# 启动时加载默认语言（如果 locales/ 已存在）
_default_path = os.path.join(_LOCALES_DIR, "zh_CN.json")
if os.path.exists(_default_path):
    set_lang("zh_CN")
