import builtins
import os

import pytest

from common import storage
from common.storage import volume_path, write_bytes


def _leftover_tmp_files(directory) -> list[str]:
    return [name for name in os.listdir(directory) if name.endswith(".tmp")]


class TestVolumePath:
    def test_basic(self):
        assert volume_path("c", "s", "v", "a", "f.jpg") == "/Volumes/c/s/v/a/f.jpg"

    def test_strips_redundant_slashes(self):
        assert volume_path("c", "s", "v", "/a/b/", "f.jpg") == "/Volumes/c/s/v/a/b/f.jpg"

    def test_skips_empty_parts(self):
        assert volume_path("c", "s", "v", "", "/", "f.jpg") == "/Volumes/c/s/v/f.jpg"


class TestWriteBytes:
    def test_writes_new_file_and_creates_dirs(self, tmp_path):
        path = tmp_path / "x" / "y" / "a.jpg"
        result = write_bytes(str(path), b"abc")
        assert path.read_bytes() == b"abc"
        assert result.size == 3
        assert result.overwritten is False
        assert _leftover_tmp_files(path.parent) == []

    def test_overwrites_existing_file(self, tmp_path):
        path = tmp_path / "a.jpg"
        path.write_bytes(b"old")
        result = write_bytes(str(path), b"new-content")
        assert path.read_bytes() == b"new-content"
        assert result.overwritten is True
        assert _leftover_tmp_files(tmp_path) == []

    def test_overwrite_false_raises_and_keeps_old_file(self, tmp_path):
        path = tmp_path / "a.jpg"
        path.write_bytes(b"old")
        with pytest.raises(FileExistsError):
            write_bytes(str(path), b"new", overwrite=False)
        assert path.read_bytes() == b"old"

    def test_mid_write_failure_keeps_old_file_and_cleans_tmp(self, tmp_path, monkeypatch):
        """寫到一半失敗：舊檔必須完整保留，且不留下暫存檔。"""
        path = tmp_path / "a.jpg"
        path.write_bytes(b"GOOD-OLD-IMAGE")
        real_open = builtins.open

        class FailingWriter:
            def __init__(self, fh):
                self.fh = fh

            def write(self, data):
                self.fh.write(data[:3])
                raise OSError("simulated mid-write failure")

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self.fh.close()

        def fake_open(file, mode="r", *args, **kwargs):
            fh = real_open(file, mode, *args, **kwargs)
            return FailingWriter(fh) if "w" in mode else fh

        monkeypatch.setattr(builtins, "open", fake_open)
        with pytest.raises(OSError, match="mid-write"):
            write_bytes(str(path), b"NEW-IMAGE-CONTENT")
        monkeypatch.undo()

        assert path.read_bytes() == b"GOOD-OLD-IMAGE"
        assert _leftover_tmp_files(tmp_path) == []

    def test_size_mismatch_keeps_old_file_and_cleans_tmp(self, tmp_path, monkeypatch):
        path = tmp_path / "a.jpg"
        path.write_bytes(b"GOOD-OLD-IMAGE")
        monkeypatch.setattr(storage.os.path, "getsize", lambda p: 1)
        with pytest.raises(OSError, match="大小不符"):
            write_bytes(str(path), b"NEW-IMAGE-CONTENT")
        monkeypatch.undo()

        assert path.read_bytes() == b"GOOD-OLD-IMAGE"
        assert _leftover_tmp_files(tmp_path) == []


def test_copy_file_streams_bytes_and_reports_overwrite(tmp_path):
    from common.storage import copy_file

    src = tmp_path / "src.zip"
    src.write_bytes(b"\x00\x01binary")
    dst = tmp_path / "out" / "nested" / "src.zip"

    first = copy_file(str(src), str(dst))
    assert dst.read_bytes() == b"\x00\x01binary"
    assert (first.size, first.overwritten) == (8, False)

    src.write_bytes(b"new")
    second = copy_file(str(src), str(dst))
    assert dst.read_bytes() == b"new" and second.overwritten
    assert [p.name for p in dst.parent.iterdir()] == ["src.zip"]  # no temp file left
