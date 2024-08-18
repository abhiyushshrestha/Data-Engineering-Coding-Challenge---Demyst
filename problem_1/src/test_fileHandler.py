import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import os
import logging
from io import StringIO
from fileHandler import Parser, Writer

class TestParser(unittest.TestCase):
    
    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    def test_parse_json(self, mock_file):
        parser = Parser()
        result = parser.parse_json("dummy_path.json")
        self.assertEqual(result, {"key": "value"})
        mock_file.assert_called_once_with("dummy_path.json")
    
    @patch("builtins.open", new_callable=mock_open, read_data="Some text content")
    def test_parse_txt(self, mock_file):
        parser = Parser()
        result = parser.parse_txt("dummy_path.txt")
        self.assertEqual(result.read(), "Some text content")
        mock_file.assert_called_once_with("dummy_path.txt", "r")

    @patch("logging.error")
    @patch("builtins.open", side_effect=Exception("File not found"))
    def test_parse_json_error(self, mock_file, mock_logging_error):
        parser = Parser()
        result = parser.parse_json("non_existent.json")
        self.assertIsNone(result)
        mock_logging_error.assert_called_once()
    
    @patch("logging.error")
    @patch("builtins.open", side_effect=Exception("File not found"))
    def test_parse_txt_error(self, mock_file, mock_logging_error):
        parser = Parser()
        result = parser.parse_txt("non_existent.txt")
        self.assertIsNone(result)
        mock_logging_error.assert_called_once()

class TestWriter(unittest.TestCase):
    
    @patch("os.path.exists", return_value=False)
    @patch("builtins.open", new_callable=mock_open)
    def test_save_csv(self, mock_file, mock_path_exists):
        writer = Writer()
        spec_file = {'ColumnNames': ['col1', 'col2']}
        data = [{'col1': 'value1', 'col2': 'value2'}, {'col1': 'value3', 'col2': 'value4'}]
        writer.save_csv(spec_file, data, "dummy_output.csv")
        mock_file.assert_called_once_with("dummy_output.csv", 'w')
        mock_path_exists.assert_called_once_with("dummy_output.csv")
        mock_file().write.assert_any_call("col1,col2\r\n")
        mock_file().write.assert_any_call("value1,value2\r\n")
        mock_file().write.assert_any_call("value3,value4\r\n")
    
    @patch("os.path.exists", return_value=True)
    @patch("logging.warning")
    def test_save_csv_file_exists(self, mock_logging_warning, mock_path_exists):
        writer = Writer()
        writer.save_csv({}, [], "dummy_output.csv")
        mock_logging_warning.assert_called_once_with("The file 'dummy_output.csv' already exists.")

    @patch("os.path.exists", return_value=False)
    @patch("logging.error")
    @patch("builtins.open", side_effect=Exception("Cannot write to file"))
    def test_save_csv_error(self, mock_file, mock_logging_error, mock_path_exists):
        writer = Writer()
        writer.save_csv({}, [], "dummy_output.csv")
        mock_logging_error.assert_called_once()

if __name__ == "__main__":
    unittest.main()
