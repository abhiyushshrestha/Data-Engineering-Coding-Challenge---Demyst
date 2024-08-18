import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2
from pyspark.sql.types import StructType, StructField, StringType

from sparkHandler import SparkHandler

class TestSparkHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        '''
        Sets up the Spark session and SparkHandler instance for testing.
        '''
        cls.spark_handler = SparkHandler(app_name='Test Spark Handler')
        cls.spark = cls.spark_handler.spark

    def setUp(self):
        '''
        Sets up a temporary DataFrame for testing.
        '''
        # Define schema and data for testing
        schema = StructType([
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("address", StringType(), True)
        ])
        data = [("John", "Doe", "123 Elm St"), ("Jane", "Doe", "456 Oak St")]

        # Create DataFrame
        self.df = self.spark.createDataFrame(data, schema)

    def test_anonymize_data(self):
        '''
        Tests the anonymization of the DataFrame.
        '''
        # Anonymize data
        anonymized_df = self.spark_handler.anonymize_data(self.df)

        # Collect the results
        results = anonymized_df.collect()

        # Check if anonymization has been applied
        for row in results:
            self.assertNotEqual(row['first_name'], 'John')
            self.assertNotEqual(row['last_name'], 'Doe')
            self.assertNotEqual(row['address'], '123 Elm St')


    @classmethod
    def tearDownClass(cls):
        '''
        Stops the Spark session after tests are complete.
        '''
        if cls.spark:
            cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
