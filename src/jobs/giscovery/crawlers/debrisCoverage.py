"""土石流潛勢溪流影響範圍圖：data.gov.tw 以 fulltext + 民國年搜尋，下載 zip。"""

from giscovery_crawler import datagov, http


def main(sink, params):
    file_name = f"{params['task_name']}.{params['file_format']}"
    url = datagov.first_url_by_roc_year(params["download_url"], params["search_keyword"], keyword_field="fulltext")
    print("* Download URL:", url)
    res = http.get(url)
    return [sink.write(file_name, res.content)]
