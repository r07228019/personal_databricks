"""與 Databricks 執行環境相關的工具。

dbutils 只存在於 notebook 的全域變數中，因此一律由呼叫端傳入，模組本身不依賴它。
"""

import os


def get_notebook_dir(dbutils) -> str:
    """回傳目前 notebook 所在目錄的絕對路徑（含 /Workspace 前綴，可直接用 open() 讀取）。"""
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return f"/Workspace{os.path.dirname(notebook_path)}"
