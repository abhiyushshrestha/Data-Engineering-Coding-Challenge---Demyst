from sparkHandler import SparkHandler
from fileHandler import FileHandler

import logging
import argparse

class Main:
    '''
    The Main class orchestrates the process of reading a CSV file, optionally generating dummy data, 
    anonymizing the data, and writing the output to a specified file.

    Methods:
        __init__():
            Initializes instances of SparkHandler and FileHandler.
        
        execute(input_file_path, output_file_path, size='xs', generated_csv_file_path='data/input_file/generated_file.csv', number_of_records=10, generate_records=False):
            Executes the process of generating dummy records (optional), reading a CSV file, anonymizing the data, and writing the results to a specified output file.
    '''

    def __init__(self):
        '''
        Initializes instances of SparkHandler and FileHandler.
        
        Returns:
            None
        '''
        self.spark_handler = SparkHandler(app_name='Anonymizing the data in CSV file')
        self.file_handler = FileHandler()
    
    def execute(self, input_file_path, output_file_path, size='xs', generated_csv_file_path='data/input_file/generated_file.csv', number_of_records=10, generate_records=False):
        '''
        Executes the complete process of optionally generating dummy records, reading a CSV file, 
        anonymizing the data, and writing the result to a specified output file.

        Params:
            input_file_path (str): Path to the input CSV file.
            output_file_path (str): Path to the output CSV file where the anonymized data will be saved.
            size (str, optional): Size of the dataset, affecting processing (default is 'xs').
            generated_csv_file_path (str, optional): Path to save the generated dummy CSV file (default is 'data/input_file/generated_file.csv').
            number_of_records (int, optional): Number of dummy records to generate (default is 10).
            generate_records (bool, optional): Whether to generate dummy records (default is False).

        Returns:
            None
        
        Raises:
            Exception: Logs an error if any issues occur during the data processing steps.
        '''
        try:
            if generate_records:
                logging.info("Processing the generation of dummy data")
                data = self.file_handler.generate_dummy_records(number_of_records=number_of_records)

                logging.info("Processing generated data to save as a CSV file")
                self.file_handler.save_csv(data=data, output_file_path=generated_csv_file_path)

            logging.info("Processing reading CSV file")
            df = self.file_handler.read_csv(spark=self.spark_handler.spark, input_file_path=input_file_path)
            # df.show(truncate=False)

            logging.info("Processing anonymizing the data")
            df = self.spark_handler.anonymize_data(df=df)
            # df.show(truncate=False)

            logging.info("Processing writing CSV file to a disk")
            self.spark_handler.write_data(df, output_file_path=output_file_path)
        except Exception as e:
            logging.error("Something went wrong. Please check the error message below:\n{e}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s : %(levelname)s : %(message)s')
    parser = argparse.ArgumentParser(description ='Getting file path and other details from the command line')
    parser.add_argument('-i', '--input_file_path', type=str, required=True, help ='a path to input file')
    parser.add_argument('-o', '--output_file_path', type=str, default='data/output_file/anonymize_data', required=True, help ='a path to output file')
    parser.add_argument('-s', '--size', type=str, default='xs', required=True, choices=['xs', 's', 'm', 'l'], help ='a string value to define size of dataset.\nxs: For file size <10Mb. \ns: For file size >=10Mb and <100Mb. \nm: For file size >=100Mb and <2gb. \nl: For file size >=2Gb. ')
    parser.add_argument('-g', '--generated_csv_file_path', type=str, default='data/input_file/generated_file.csv', required=False, help ='a path to generated CSV file')
    parser.add_argument('-gr', '--generate_records', type=bool, default=False, required=False, help ='a boolean value to generate dummy records or not')
    parser.add_argument('-n', '--number_of_records', type=int, default=10, required=False, help ='an integer value to define number of records to generate')

    args = parser.parse_args()
    input_file_path = args.input_file_path
    output_file_path = args.output_file_path
    size = args.size
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
              output_file_path=output_file_path,
              size=size,
              generated_csv_file_path=generated_csv_file_path,
              number_of_records=number_of_records,
              generate_records=generate_records)

