"""台電再生能源發電量（太陽能）：download_url 直接指向 csv，經 pandas 重新輸出。"""

from io import StringIO

import pandas as pd

from giscovery_crawler import http


def main(sink, params):
    file_name = f"{params['task_name']}.{params['file_format']}"
    print("* Download URL:", params["download_url"])
    res = http.get(params["download_url"])
    df = pd.read_csv(StringIO(res.text))
    buf = StringIO()
    df.to_csv(buf, index=False)
    return [sink.write(file_name, buf.getvalue(), encoding=params["file_encoding"])]
