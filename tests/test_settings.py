import json
import logging
import unittest.mock as mock

from toolkit.resourcecontroller import Config

def test_remove_json_extension():
    # Provide dummy values for the required arguments
    config = Config("dummy_url", "dummy_secret", "dummy_path")

    # Test case where .json extension is present
    file_path_with_json = "example.json"
    assert config._remove_json_extension(file_path_with_json) == "example"

    # Test case where .json extension is not present
    file_path_without_json = "example.txt"
    assert config._remove_json_extension(file_path_without_json) is None

    # Test case with an empty string
    empty_file_path = ""
    assert config._remove_json_extension(empty_file_path) is None

def test_load_config_valid_json(tmp_path):
    config = Config("dummy_url", "dummy_secret", "dummy_path")
    # Create a temporary JSON file with valid content
    file = tmp_path / "config.json"
    file.write_text(json.dumps({"key": "value"}))

    # Test loading valid JSON
    assert config._load_config(str(file)) == {"key": "value"}

def test_load_config_file_not_found(caplog):
    config = Config("dummy_url", "dummy_secret", "dummy_path")
    non_existent_file = "nonexistent.json"

    # Test loading a non-existent file
    with caplog.at_level(logging.ERROR):
        assert config._load_config(non_existent_file) is None
    assert "File not found: " + non_existent_file in caplog.text

def test_load_config_invalid_json(tmp_path, caplog):
    config = Config("dummy_url", "dummy_secret", "dummy_path")
    # Create a temporary JSON file with invalid content
    file = tmp_path / "invalid.json"
    file.write_text("not a valid json")

    # Test loading invalid JSON
    with caplog.at_level(logging.ERROR):
        assert config._load_config(str(file)) is None
    assert "JSON decoding error:" in caplog.text    