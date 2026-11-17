"""醫療機構與人員基本資料：data.gov.tw 以 title 搜尋，下載 ods。"""

from giscovery_crawler import datagov, http


def main(sink, params):
    file_name = f"{params['task_name']}.{params['file_format']}"
    url = datagov.first_url_by_title(params["download_url"], params["search_keyword"])
    print("* Download URL:", url)
    res = http.get(url)
    return [sink.write(file_name, res.content)]
