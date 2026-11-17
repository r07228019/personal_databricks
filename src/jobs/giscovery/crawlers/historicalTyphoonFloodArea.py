"""歷年颱風淹水範圍：download_url 直接指向 kml。"""

from giscovery_crawler import http


def main(sink, params):
    file_name = f"{params['task_name']}.{params['file_format']}"
    print("* Download URL:", params["download_url"])
    res = http.get(params["download_url"])
    return [sink.write(file_name, res.content)]
