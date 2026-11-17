"""把下載內容寫進 UC Volume，取代原本的 S3 crawlersink bucket。

每支爬蟲的檔案都放在 /Volumes/<catalog>/<schema>/<volume>/<task_name>/ 底下，
檔名規則沿用原本 S3 的 key（多數是 <task_name>.<file_format>）。
"""

import os
from dataclasses import dataclass, field

from common.storage import WriteResult, volume_path, write_bytes


@dataclass
class VolumeSink:
    catalog: str
    schema: str
    volume: str
    task_name: str
    written: list[WriteResult] = field(default_factory=list)

    @property
    def base_dir(self) -> str:
        return volume_path(self.catalog, self.schema, self.volume, self.task_name)

    def path_for(self, file_name: str) -> str:
        return os.path.join(self.base_dir, file_name)

    def exists(self, file_name: str) -> bool:
        return os.path.exists(self.path_for(file_name))

    def write(self, file_name: str, content: bytes | str, *, encoding: str = "utf-8") -> str:
        """寫入檔案（同名直接覆蓋），回傳完整路徑。str 會依 encoding 編碼後寫入。"""
        if isinstance(content, str):
            content = content.encode(encoding)
        result = write_bytes(self.path_for(file_name), content, overwrite=True)
        self.written.append(result)
        print(f"* {'overwrote' if result.overwritten else 'saved'} {result.size:,} bytes -> {result.path}")
        return result.path
