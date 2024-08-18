import unittest
from unittest.mock import patch
import logging

from utils import Utils

class TestUtils(unittest.TestCase):
    
    def setUp(self):
        self.utils = Utils()
    
    def test_fixed_width_parser_success(self):
        # Test data
        spec_file = {
            'ColumnNames': ['col1', 'col2', 'col3'],
            'Offsets': [5, 10, 5]
        }
        input_file = [
            '12345abcdefghij67890',
            '09876zyxwvutsrqponm'
        ]
        
        expected_output = [
            {'col1': '12345', 'col2': 'abcdefghij', 'col3': '67890'},
            {'col1': '09876', 'col2': 'zyxwvutsrq', 'col3': 'ponm'}
        ]
        
        result = self.utils.fixed_width_parser(spec_file, input_file)
        self.assertEqual(result, expected_output)
    
    @patch("logging.error")
    def test_fixed_width_parser_error(self, mock_logging_error):
        # Test with invalid spec_file that causes an exception
        spec_file = {
            'ColumnNames': ['col1', 'col2'],  # Fewer columns than offsets
            'Offsets': [5, 10, 5]
        }
        input_file = [
            '12345abcdefghij67890'
        ]
        
        result = self.utils.fixed_width_parser(spec_file, input_file)
        self.assertIsNone(result)
        mock_logging_error.assert_called_once()

    def test_fixed_width_parser_empty_input(self):
        # Test with empty input_file
        spec_file = {
            'ColumnNames': ['col1', 'col2', 'col3'],
            'Offsets': [5, 10, 5]
        }
        input_file = []
        
        expected_output = []
        
        result = self.utils.fixed_width_parser(spec_file, input_file)
        self.assertEqual(result, expected_output)

    def test_fixed_width_parser_mismatched_offsets_and_data(self):
        # Test with input data that doesn't match the offsets
        spec_file = {
            'ColumnNames': ['col1', 'col2', 'col3'],
            'Offsets': [5, 10, 5]
        }
        input_file = [
            '12345abcd'  # Shorter than the sum of offsets
        ]
        
        expected_output = [
            {'col1': '12345', 'col2': 'abcd', 'col3': ''}  # Last field should be empty
        ]
        
        result = self.utils.fixed_width_parser(spec_file, input_file)
        self.assertEqual(result, expected_output)

if __name__ == "__main__":
    unittest.main()
