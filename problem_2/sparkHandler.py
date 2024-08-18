from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import sha2

class SparkHandler:

    def __init__(self, app_name):
        try:
            logging.info("Initializing spark session")
            self.spark = SparkSession.builder.appName(app_name).getOrCreate()
            logging.info("Spark session initiated successfully.")
        except Exception as e:
            logging.error("Spark sesssion failed to initiate. Please check the error message below:\n{e}")
            self.spark = None

    def anonymize_data(self, df):
        try:
            df = df.withColumn('first_name', sha2(df['first_name'], 256)) \
                    .withColumn('last_name', sha2(df['last_name'], 256)) \
                    .withColumn('address', sha2(df['address'], 256))
            return df
        except Exception as e:
            logging.error(f"Error occured while anonymizing the data. Plese check the error message below: \n{e}")
    
    def write_data(self, df, output_file_path='data/output_file/anonymize_data'):
        try:
            df.write.mode('overwrite').option("header",True).csv(output_file_path)
        except Exception as e:
            logging.error(f"Error occured while writing the anonymized data to CSV file. Plese check the error message below: \n{e}")
    
