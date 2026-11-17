"""台灣離岸風電潛力場址：data.gov.tw 以 title 搜尋（需帶瀏覽器 headers），csv 經 pandas 重新輸出。"""

from io import StringIO

import pandas as pd

from giscovery_crawler import datagov, http


def main(sink, params):
    file_name = f"{params['task_name']}.{params['file_format']}"
    headers = params.get("browser_headers")
    url = datagov.first_url_by_title(params["download_url"], params["search_keyword"], headers=headers)
    print("* Download URL:", url)
    res = http.get(url, headers=headers)
    df = pd.read_csv(StringIO(res.text))
    buf = StringIO()
    df.to_csv(buf, index=False)
    return [sink.write(file_name, buf.getvalue(), encoding=params["file_encoding"])]
