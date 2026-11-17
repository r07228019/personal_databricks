"""to_etl=false 的 task：把爬蟲下載的檔案原封不動複製到 etldestined Volume。

對應原本 glue-trigger Lambda 的 copy_file（S3 copy_object，crawlersink → etldestined，key 不變）。
Lambda 是每個 S3 ObjectCreated 事件複製一個檔；這裡由爬蟲 job 爬完後觸發一次，
改成比對整個 <task_name>/ 目錄，只複製「目的端沒有」或「來源比目的端新／大小不同」的檔案，
所以爬蟲略過沒重下載的檔案（例如 CWB 已存在的檔）不會重複複製，效果與 Lambda 相同。
"""

import os

from common.storage import WriteResult, copy_file


def list_source_files(src_dir: str) -> list[str]:
    """列出 src_dir 第一層的檔名（排序），略過子目錄與 `.` 開頭的檔（write_bytes 寫到一半的暫存檔）。"""
    if not os.path.isdir(src_dir):
        raise FileNotFoundError(f"找不到來源目錄：{src_dir}")
    return sorted(
        name for name in os.listdir(src_dir) if not name.startswith(".") and os.path.isfile(os.path.join(src_dir, name))
    )


def needs_copy(src: str, dst: str) -> bool:
    if not os.path.exists(dst):
        return True
    s, d = os.stat(src), os.stat(dst)
    return s.st_size != d.st_size or s.st_mtime > d.st_mtime


def copy_task_files(src_dir: str, dst_dir: str, file_names: list[str] | None = None) -> list[WriteResult]:
    """把 src_dir 的檔案複製到 dst_dir（同檔名），回傳實際有複製的檔案。

    file_names 為 None 時處理 src_dir 第一層全部的檔；來源一個檔都沒有時拋錯（代表爬蟲沒產出）。
    """
    names = list_source_files(src_dir) if file_names is None else file_names
    if not names:
        raise FileNotFoundError(f"來源目錄沒有任何檔案：{src_dir}")
    copied = []
    for name in names:
        src, dst = os.path.join(src_dir, name), os.path.join(dst_dir, name)
        if not needs_copy(src, dst):
            print(f"* skip unchanged {dst}")
            continue
        result = copy_file(src, dst)
        print(f"* {'overwrote' if result.overwritten else 'copied'} {result.size:,} bytes -> {result.path}")
        copied.append(result)
    return copied
