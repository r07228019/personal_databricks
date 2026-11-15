"""依 config 組出 CWA 圖檔 URL 與 Volume 目的地路徑。"""

from typing import Any

from common.storage import volume_path

from .params import RunParams


def build_source_url(source_cfg: dict[str, Any], params: RunParams) -> str:
    """config.source.url_template 可用的佔位符：{date}（依 url_date_format）、{hour}（整數）。"""
    return source_cfg["url_template"].format(
        date=params.data_date.strftime(source_cfg["url_date_format"]),
        hour=params.data_hour,
    )


def build_dest_path(dest_cfg: dict[str, Any], params: RunParams) -> str:
    """config.destination.file_name_template 可用的佔位符：{date}（依 file_date_format）、{hour}（整數）。"""
    file_name = dest_cfg["file_name_template"].format(
        date=params.data_date.strftime(dest_cfg["file_date_format"]),
        hour=params.data_hour,
    )
    return volume_path(dest_cfg["catalog"], dest_cfg["schema"], dest_cfg["volume"], dest_cfg["sub_dir"], file_name)
