import pandas as pd
import csv
import logging
import os
from faker import Faker

class FileHandler:

    def read_csv(self, spark, input_file_path):
        try:
            if spark is None:
                raise Exception("Spark session is not initiated")
            else:
                df = spark.read.csv(input_file_path, header=True, inferSchema=True)
                return df
        except Exception as e:
            logging.error(f"Error occured while reading CSV file. Please check the error below:\n{e}")
    
    def generate_dummy_records(self, number_of_records):
        try:
            fake = Faker()
            data = []
            
            for i in range(0, number_of_records):
                record = dict()
                record['first_name'] = fake.first_name()
                record['last_name'] = fake.last_name()
                record['address'] = fake.street_address()
                record['date_of_birth'] = str(fake.date_of_birth())
                data.append(record)
            return data
        except Exception as e:
            logging.error(f"Error occured while generating dummy data. Plese check the error message below: \n{e}")
    
    
    def save_csv(self, data, output_file_path):
        '''
        Writes the provided data to a CSV file at the specified output file path.

        Params:
            spec_file (dict): A dictionary containing the specification, including column names for the CSV.
            data (list of dict): The data to be written to the CSV file, where each dictionary represents a row.
            output_file_path (str): The file path where the output CSV file will be saved.

        Returns:
            None

        Raises:
            Logs an error if an exception occurs during file writing.
            Logs a warning if the output file already exists.
        '''
        try:
            if not os.path.exists(output_file_path): 
                column_header_names = list(data[0].keys())
                with open(output_file_path, 'w' , encoding='utf-8') as csvfile: 
                    writer = csv.DictWriter(csvfile, fieldnames=column_header_names)
                    writer.writeheader()
                    writer.writerows(data) 
            else: 
                logging.warning(f"The file '{output_file_path}' already exists.")
                column_header_names = list(data[0].keys())
                with open(output_file_path, 'ab' , encoding='utf-8') as csvfile: 
                    writer = csv.DictWriter(csvfile, fieldnames=column_header_names)
                    writer.writeheader()
                    writer.writerows(data)
        except Exception as e:
            logging.error(f"Something went wrong. Plese check the error message below: \n{e}")



    