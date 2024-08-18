import unittest
import os
import tempfile
import pandas as pd
from pyspark.sql import SparkSession
from fileHandler import FileHandler

class TestFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        '''Sets up the Spark session and FileHandler instance for testing.'''
        cls.spark = SparkSession.builder.appName("TestFileHandler").getOrCreate()
        cls.file_handler = FileHandler()

    def setUp(self):
        '''Sets up temporary file paths for testing.'''
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        self.temp_file_path = self.temp_file.name
        self.temp_file.close()

    
    def test_read_csv(self):
        '''Tests reading a CSV file into a Spark DataFrame.'''
        # Create a sample CSV file
        df = pd.DataFrame({
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'address': ['123 Elm St', '456 Oak St']
        })
        df.to_csv(self.temp_file_path, index=False)

        # Read the CSV file using FileHandler
        result_df = self.file_handler.read_csv(self.spark, self.temp_file_path)

        # Collect the Spark DataFrame rows
        result = result_df.collect()

        # Verify the DataFrame contents
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['first_name'], 'John')
        self.assertEqual(result[1]['last_name'], 'Smith')

    def test_generate_dummy_records(self):
        '''Tests generating dummy records using the Faker library.'''
        number_of_records = 5
        records = self.file_handler.generate_dummy_records(number_of_records)

        # Verify the number of records generated
        self.assertEqual(len(records), number_of_records)

        # Check that each record is a dictionary with the expected keys
        for record in records:
            self.assertIn('first_name', record)
            self.assertIn('last_name', record)
            self.assertIn('address', record)
            self.assertIn('date_of_birth', record)
    
    def tearDown(self):
        '''Cleans up temporary files after each test.'''
        if os.path.exists(self.temp_file_path):
            os.remove(self.temp_file_path)

if __name__ == '__main__':
    unittest.main()
