"""各縣市醫院平均住院日數：data.gov.tw 搜尋後挑年份最新的 csv，經 pandas 重新輸出。"""

from io import StringIO

import pandas as pd

from giscovery_crawler import datagov, http


def main(sink, params):
    file_name = f"{params['task_name']}.{params['file_format']}"
    url = datagov.newest_year_url_by_title(params["download_url"], params["search_keyword"])
    print("* Download URL:", url)
    res = http.get(url)
    df = pd.read_csv(StringIO(res.text), index_col=False)
    buf = StringIO()
    df.to_csv(buf, index=False)
    return [sink.write(file_name, buf.getvalue(), encoding=params["file_encoding"])]
