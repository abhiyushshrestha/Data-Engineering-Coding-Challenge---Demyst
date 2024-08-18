from sparkHandler import SparkHandler
from fileHandler import FileHandler

import logging
import argparse

class Main:

    def __init__(self):
        self.spark_handler = SparkHandler(app_name='Anonymizing the data in CSV file')
        self.file_handler = FileHandler()
    
    def execute(self, input_file_path, output_file_path, generated_csv_file_path='data/input_file/generated_file.csv', number_of_records=10, generate_records=False):
        try:
            if generate_records:
                logging.info("Generating dummy data")
                data = self.file_handler.generate_dummy_records(number_of_records=number_of_records)

                logging.info("Saving generated data to CSV file")
                self.file_handler.save_csv(data=data, output_file_path=generated_csv_file_path)

            df = self.file_handler.read_csv(spark=self.spark_handler.spark, input_file_path=input_file_path)
            # df.show(truncate=False)
            df = self.spark_handler.anonymize_data(df=df)
            # df.show(truncate=False)
            self.spark_handler.write_data(df, output_file_path=output_file_path)
        except Exception as e:
            logging.error("Something went wrong. Please check the error message below:\n{e}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s : %(levelname)s : %(message)s')
    parser = argparse.ArgumentParser(description ='Getting file path and other details from the command line')
    parser.add_argument('-i', '--input_file_path', type=str, required=True, help ='a path to input file')
    parser.add_argument('-o', '--output_file_path', type=str, default='data/output_file/anonymize_data', required=True, help ='a path to output file')
    parser.add_argument('-g', '--generated_csv_file_path', type=str, default='data/input_file/generated_file.csv', required=True, help ='a path to generated CSV file')
    parser.add_argument('-gr', '--generate_records', type=bool, default=False, required=False, help ='a boolean value to generate dummy records or not')
    parser.add_argument('-n', '--number_of_records', type=int, default=10, required=False, help ='an integer value to define number of records to generate')

    args = parser.parse_args()
    input_file_path = args.input_file_path
    output_file_path = args.output_file_path
    generated_csv_file_path = args.generated_csv_file_path
    generate_records = args.generate_records
    number_of_records = args.number_of_records
    
    # generated_csv_file_path = "data/input_file/generated_file.csv"
    # input_file_path = "data/input_file/generated_file.csv"
    # output_file_path = "data/output_file/anonymize_data.csv"
    # number_of_records = 10
    # generate_records = True
    
    m = Main()

    m.execute(input_file_path=input_file_path, 
              output_file_path = output_file_path,
              generated_csv_file_path=generated_csv_file_path,
              number_of_records=number_of_records,
              generate_records=generate_records)



# from pyspark.sql import SparkSession

# # Step 1: Initialize Spark session
# spark = SparkSession.builder \
#     .appName("Distributed CSV Processing") \
#     .master("local[*]") \
#     .getOrCreate()

# # Step 2: Read CSV file in a distributed manner
# df = spark.read.csv("path/to/large_file.csv", header=True, inferSchema=True)

# # Step 3: Perform some transformations
# df_filtered = df.filter(df["column_name"] > 100)
# df_grouped = df_filtered.groupBy("another_column").count()

# # Step 4: Write the output back to a CSV
# df_grouped.write.csv("path/to/output_directory", header=True)

# # Stop the Spark session
# spark.stop()

