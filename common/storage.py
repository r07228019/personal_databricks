"""Unity Catalog Volume 路徑組裝與寫檔。"""

import os
import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class WriteResult:
    path: str
    size: int
    overwritten: bool


def volume_path(catalog: str, schema: str, volume: str, *parts: str) -> str:
    """組出 /Volumes/<catalog>/<schema>/<volume>/<parts...>，自動去除各段多餘的斜線。"""
    segments = [catalog, schema, volume, *parts]
    cleaned = [s.strip("/") for s in segments if s and s.strip("/")]
    return "/Volumes/" + "/".join(cleaned)


def write_bytes(path: str, content: bytes, *, overwrite: bool = True) -> WriteResult:
    """安全寫入檔案（自動建立上層目錄）。

    流程：先寫到同目錄的暫存檔 → 確認大小與內容長度一致 → 以 os.replace 換成正式檔名。
    任何一步失敗都會刪除暫存檔並拋出例外，原本的正式檔案維持不動，不會留下寫一半的殘檔。
    overwrite=False 且檔案已存在時拋出 FileExistsError。
    """
    overwritten = os.path.exists(path)
    if overwritten and not overwrite:
        raise FileExistsError(f"檔案已存在：{path}")

    dir_name, base_name = os.path.split(path)
    os.makedirs(dir_name, exist_ok=True)
    tmp_path = os.path.join(dir_name, f".{base_name}.{uuid.uuid4().hex}.tmp")
    try:
        with open(tmp_path, "wb") as f:
            f.write(content)
        size = os.path.getsize(tmp_path)
        if size != len(content):
            raise OSError(f"暫存檔大小不符：{tmp_path}（{size} != {len(content)}）")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    return WriteResult(path=path, size=size, overwritten=overwritten)
