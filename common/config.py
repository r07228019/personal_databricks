"""讀取 JSON 設定檔。"""

import json
import os
from typing import Any

DEFAULT_CONFIG_FILE_NAME = "config.json"


def resolve_config_path(explicit_path: str, default_dir: str, file_name: str = DEFAULT_CONFIG_FILE_NAME) -> str:
    """有指定路徑就用指定的；空白則回傳 default_dir 底下的 file_name。"""
    explicit_path = (explicit_path or "").strip()
    return explicit_path if explicit_path else os.path.join(default_dir, file_name)


def load_json_config(path: str) -> dict[str, Any]:
    """讀取 UTF-8 JSON 設定檔，檔案不存在時給出清楚的錯誤訊息。"""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"找不到設定檔：{path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)
