from unittest.mock import patch

from giscovery_crawler.sink import VolumeSink


def test_paths():
    sink = VolumeSink(catalog="test", schema="giscovery", volume="crawlersink", task_name="cathayATM")
    assert sink.base_dir == "/Volumes/test/giscovery/crawlersink/cathayATM"
    assert sink.path_for("cathayATM.json") == "/Volumes/test/giscovery/crawlersink/cathayATM/cathayATM.json"


def test_write_str_and_bytes(tmp_path):
    sink = VolumeSink(catalog="c", schema="s", volume="v", task_name="t")
    with patch("giscovery_crawler.sink.volume_path", return_value=str(tmp_path / "t")):
        p1 = sink.write("a.txt", "中文", encoding="utf-8")
        p2 = sink.write("b.bin", b"\x00\x01")
        assert sink.exists("a.txt") and sink.exists("b.bin") and not sink.exists("c")
    assert open(p1, "rb").read() == "中文".encode()
    assert open(p2, "rb").read() == b"\x00\x01"
    assert [w.size for w in sink.written] == [6, 2]
