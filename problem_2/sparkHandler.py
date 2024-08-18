from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import sha2

class SparkHandler:
    '''
    The SparkHandler class provides methods for initializing a Spark session, 
    anonymizing data in a DataFrame, and writing the anonymized data to a CSV file.

    Methods:
        __init__(app_name):
            Initializes the Spark session with the given application name.
        
        anonymize_data(df):
            Anonymizes sensitive columns in the provided DataFrame.
        
        write_data(df, output_file_path='data/output_file/anonymize_data'):
            Writes the anonymized DataFrame to a CSV file at the specified output file path.
    '''

    def __init__(self, app_name):
        '''
        Initializes the Spark session with the given application name.

        Params:
            app_name (str): The name of the Spark application.

        Returns:
            None

        Raises:
            Logs an error if an exception occurs during Spark session initialization.
        '''
        try:
            logging.info("Initializing spark session")
            self.spark = SparkSession.builder.appName(app_name).getOrCreate()
            logging.info("Spark session initiated successfully.")
        except Exception as e:
            logging.error("Spark sesssion failed to initiate. Please check the error message below:\n{e}")
            self.spark = None

    def anonymize_data(self, df):
        '''
        Anonymizes the data in the DataFrame by hashing sensitive columns.

        Params:
            df (pyspark.sql.DataFrame): The DataFrame containing the data to be anonymized.

        Returns:
            pyspark.sql.DataFrame: The DataFrame with anonymized data.

        Raises:
            Logs an error if an exception occurs during the anonymization process.
        '''
        try:
            logging.info("Anonymizing the data")
            df = df.withColumn('first_name', sha2(df['first_name'], 256)) \
                    .withColumn('last_name', sha2(df['last_name'], 256)) \
                    .withColumn('address', sha2(df['address'], 256))
            logging.info("Anonymizing the data successful")
            return df
        except Exception as e:
            logging.error(f"Error occured while anonymizing the data. Plese check the error message below: \n{e}")
    
    def write_data(self, df, output_file_path='data/output_file/anonymize_data'):
        '''
        Writes the anonymized DataFrame to a CSV file.

        Params:
            df (pyspark.sql.DataFrame): The DataFrame containing the anonymized data.
            output_file_path (str, optional): The path where the CSV file will be saved (default is 'data/output_file/anonymize_data').

        Returns:
            None

        Raises:
            Logs an error if an exception occurs during the file writing process.
        '''
        try:
            logging.info("Writing the anonymize data to CSV file.")
            df.write.mode('overwrite').option("header",True).csv(output_file_path)
            logging.info("The CSV file saved to the disk successfully.")
        except Exception as e:
            logging.error(f"Error occured while writing the anonymized data to CSV file. Plese check the error message below: \n{e}")
    
