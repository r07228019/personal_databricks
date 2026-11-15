import json
import os

import pytest

from common.config import DEFAULT_CONFIG_FILE_NAME, load_json_config, resolve_config_path


class TestResolveConfigPath:
    def test_explicit_path_wins(self):
        assert resolve_config_path("/a/b/c.json", "/default") == "/a/b/c.json"

    def test_explicit_path_is_stripped(self):
        assert resolve_config_path("  /a/c.json  ", "/default") == "/a/c.json"

    @pytest.mark.parametrize("blank", ["", "   ", None])
    def test_blank_falls_back_to_default_dir(self, blank):
        assert resolve_config_path(blank, "/default") == os.path.join("/default", DEFAULT_CONFIG_FILE_NAME)

    def test_custom_file_name(self):
        assert resolve_config_path("", "/default", "other.json") == "/default/other.json"


class TestLoadJsonConfig:
    def test_loads_utf8_json(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text(json.dumps({"name": "溫度"}, ensure_ascii=False), encoding="utf-8")
        assert load_json_config(str(path)) == {"name": "溫度"}

    def test_missing_file_raises_with_path(self, tmp_path):
        missing = tmp_path / "nope.json"
        with pytest.raises(FileNotFoundError, match="nope.json"):
            load_json_config(str(missing))
