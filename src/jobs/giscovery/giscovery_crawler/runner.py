"""依 task_name 載入 crawlers/<task_name>.py 並呼叫其 main()。

對應原本 Lambda 的 lambda_function.py：從 S3 下載腳本、從 DynamoDB 取設定、動態載入模組後呼叫 main()。
這裡腳本就在 repo 裡（crawlers/ 目錄），設定來自 Delta table。

爬蟲模組的介面：
    def main(sink, params) -> list[str]              # 一般爬蟲
    def main(sink, params, secrets) -> list[str]     # 需要密碼的爬蟲（如 CWB），secrets(scope, key) -> str
"""

import importlib.util
import inspect
import os
from collections.abc import Callable
from types import ModuleType
from typing import Any

from .sink import VolumeSink

CRAWLERS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "crawlers")

SecretGetter = Callable[[str, str], str]


def load_crawler_module(task_name: str, crawlers_dir: str = CRAWLERS_DIR) -> ModuleType:
    path = os.path.join(crawlers_dir, f"{task_name}.py")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"找不到爬蟲腳本：{path}")
    spec = importlib.util.spec_from_file_location(f"crawlers.{task_name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "main"):
        raise AttributeError(f"{path} 沒有 main(sink, params) 函式")
    return module


def run_crawler(
    task_name: str,
    params: dict[str, Any],
    sink: VolumeSink,
    *,
    secrets: SecretGetter | None = None,
    crawlers_dir: str = CRAWLERS_DIR,
) -> Any:
    """執行單一爬蟲，回傳 main() 的結果（通常是寫入的路徑清單）。"""
    if params.get("task_name") != task_name:
        raise ValueError(f"設定的 task_name={params.get('task_name')!r} 與要執行的 {task_name!r} 不一致")
    module = load_crawler_module(task_name, crawlers_dir)
    print(f"===== Executing {task_name}.main() =====")
    if "secrets" in inspect.signature(module.main).parameters:
        if secrets is None:
            raise ValueError(f"{task_name} 需要 secrets，但呼叫端沒有提供")
        return module.main(sink, params, secrets)
    return module.main(sink, params)
