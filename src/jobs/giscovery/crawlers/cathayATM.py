"""國泰世華 ATM 據點：呼叫 JSON API，取出 Atm 陣列存成 json。"""

import json

from giscovery_crawler import http


def main(sink, params):
    file_name = f"{params['task_name']}.{params['file_format']}"
    res = http.get(params["download_url"])
    atm_list = res.json()["Atm"]
    content = json.dumps(atm_list, ensure_ascii=False, indent=2)
    return [sink.write(file_name, content, encoding=params["file_encoding"])]
