import json 
import csv
import os
import logging 

class Parser:
    '''
    The Parser class provides methods for reading and parsing files in different formats, 
    specifically JSON and text files.

    Methods:
        parse_json(spec_file_path):
            Reads and parses a JSON file from the given file path.
        
        parse_txt(input_file_path):
            Reads and returns the content of a text file from the given file path.
    '''
    def parse_json(self, spec_file_path):
        '''
        Reads and parses a JSON file from the provided file path.

        Params:
            spec_file_path (str): The file path to the JSON specification file.

        Returns:
            dict: The parsed content of the JSON file as a dictionary.

        Raises:
            Logs an error if an exception occurs during file reading or parsing.
        '''
        try:
            spec_file = open(spec_file_path)
            spec_file = json.load(spec_file)
            return spec_file
        except Exception as e:
            logging.error(f"Something went wrong. Plese check the error message below: \n{e}")

    def parse_txt(self, input_file_path):
        '''
        Reads and returns the content of a text file from the provided file path.

        Params:
            input_file_path (str): The file path to the text input file.

        Returns:
            file object: The file object for the opened text file.

        Raises:
            Logs an error if an exception occurs during file reading.
        '''
        try:
            input_file = open(input_file_path, "r", encoding='windows-1252')
            return input_file
        except Exception as e:
            logging.error(f"Something went wrong. Plese check the error message below: \n{e}")

class Writer:
    '''
    The Writer class provides methods for writing data to CSV files.

    Methods:
        save_csv(spec_file, data, output_file_path):
            Writes the provided data to a CSV file at the specified output file path.
    '''
    def save_csv(self, spec_file, data, output_file_path):
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
                with open(output_file_path, 'w' , encoding='utf-8') as csvfile: 
                    writer = csv.DictWriter(csvfile, fieldnames=spec_file['ColumnNames'])
                    writer.writeheader()
                    writer.writerows(data) 
            else: 
                logging.warning(f"The file '{output_file_path}' already exists.")
        except Exception as e:
            logging.error(f"Something went wrong. Plese check the error message below: \n{e}")


