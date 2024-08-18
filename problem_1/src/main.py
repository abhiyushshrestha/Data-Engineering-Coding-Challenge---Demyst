from fileHandler import Parser, Writer
from utils import Utils

import logging
import argparse


class Main:
    '''
    The Main class orchestrates the process of reading a fixed-width file, parsing it according to a given specification,
    and then writing the parsed data into a CSV file. It utilizes the Parser, Utils, and Writer classes to accomplish 
    these tasks.

    Attributes:
        parser (Parser): An instance of the Parser class responsible for parsing the spec and input files.
        utils (Utils): An instance of the Utils class that processes the fixed-width file based on the parsed spec.
        writer (Writer): An instance of the Writer class responsible for writing the processed data into a CSV file.

    Methods:
        execute(spec_file_path, input_file_path, output_file_path):
            Parses the specification and input files, processes the fixed-width file, and writes the output to a CSV file.
    '''
    def __init__(self):
        self.parser = Parser()
        self.utils = Utils()
        self.writer = Writer()

    def execute(self, spec_file_path, input_file_path, output_file_path):
        '''
        Executes the main workflow of parsing, processing, and writing the fixed-width file data.

        Params:
            spec_file_path (str): The file path to the specification file in JSON format.
            input_file_path (str): The file path to the input fixed-width text file.
            output_file_path (str): The file path where the output CSV file will be saved.

        Returns:
            None
        '''
        logging.info(f"Parsing specification file: {spec_file_path}")
        spec_file = self.parser.parse_json(spec_file_path)

        logging.info(f"Parsing input file: {input_file_path}")
        input_file = self.parser.parse_txt(input_file_path)

        logging.info(f"Processing fixed width file")
        data = self.utils.fixed_width_parser(spec_file, input_file)

        logging.info(f"Writing the data into the CSV file: {output_file_path}")
        self.writer.save_csv(spec_file=spec_file, data=data, output_file_path=output_file_path)
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s : %(levelname)s : %(message)s')

    parser = argparse.ArgumentParser(description ='Getting file path from the command line')
    parser.add_argument('-i', '--input_file_path', type=str, required=True, help ='a path to input file')
    parser.add_argument('-o', '--output_file_path', type=str, required=True, help ='a path to output file')
    parser.add_argument('-s', '--spec_file_path', type=str, required=True, help ='a path to specification file')
    args = parser.parse_args()
    spec_file_path = args.spec_file_path
    input_file_path = args.input_file_path
    output_file_path = args.output_file_path
    
    m = Main()
    # spec_file_path = "data/spec_file/spec.json"
    # input_file_path = "data/input_file/input.txt"
    # output_file_path = "data/output_file/output.csv"
    m.execute(spec_file_path=spec_file_path, input_file_path=input_file_path, output_file_path=output_file_path)