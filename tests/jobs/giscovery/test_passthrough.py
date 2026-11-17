import os

import pytest

from giscovery_crawler.passthrough import copy_task_files, list_source_files


def _write(path, content: bytes, mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if mtime is not None:
        os.utime(path, (mtime, mtime))


def test_copies_all_files_unchanged_and_skips_hidden_and_dirs(tmp_path):
    src, dst = tmp_path / "sink" / "debrisCoverage", tmp_path / "dest" / "debrisCoverage"
    _write(src / "debrisCoverage.zip", b"PK\x03\x04binary")
    _write(src / ".debrisCoverage.zip.abc.tmp", b"partial")
    (src / "subdir").mkdir()

    copied = copy_task_files(str(src), str(dst))

    assert [os.path.basename(r.path) for r in copied] == ["debrisCoverage.zip"]
    assert (dst / "debrisCoverage.zip").read_bytes() == b"PK\x03\x04binary"
    assert sorted(p.name for p in dst.iterdir()) == ["debrisCoverage.zip"]  # no temp file left behind


def test_skips_unchanged_and_recopies_newer_or_resized(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    _write(src / "same.json", b"1", mtime=1000)
    _write(dst / "same.json", b"1", mtime=2000)
    _write(src / "newer.json", b"2", mtime=3000)
    _write(dst / "newer.json", b"x", mtime=2000)
    _write(src / "resized.json", b"333", mtime=1000)
    _write(dst / "resized.json", b"3", mtime=2000)

    copied = copy_task_files(str(src), str(dst))

    assert sorted(os.path.basename(r.path) for r in copied) == ["newer.json", "resized.json"]
    assert all(r.overwritten for r in copied)
    assert (dst / "newer.json").read_bytes() == b"2" and (dst / "resized.json").read_bytes() == b"333"


def test_explicit_file_names(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    _write(src / "a.kml", b"a")
    _write(src / "b.kml", b"b")
    copy_task_files(str(src), str(dst), ["b.kml"])
    assert [p.name for p in dst.iterdir()] == ["b.kml"]


def test_missing_or_empty_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list_source_files(str(tmp_path / "nope"))
    (tmp_path / "empty").mkdir()
    with pytest.raises(FileNotFoundError):
        copy_task_files(str(tmp_path / "empty"), str(tmp_path / "dst"))
