import logging 

class Utils:
    '''
    The Utils class provides utility functions for processing fixed-width files. Specifically, it includes methods 
    for parsing a fixed-width file into a structured format based on a provided specification.

    Methods:
        fixed_width_parser(spec_file, input_file):
            Parses a fixed-width file according to the specified column offsets and returns the data as a list of dictionaries.
    '''
    def fixed_width_parser(self, spec_file, input_file):
        '''
        Parses a fixed-width file based on a provided specification and returns the data in a structured format.

        Params:
            spec_file (dict): A dictionary containing the specification for parsing the fixed-width file. 
                              Expected keys include 'ColumnNames' and 'Offsets'.
            input_file (list of str): A list of strings where each string is a record from the fixed-width file.

        Returns:
            list of dict: A list of dictionaries where each dictionary represents a parsed record from the input file. 
                          The keys in the dictionary are derived from 'ColumnNames' in the spec file, and the values 
                          are the corresponding data from the fixed-width file.

        Raises:
            Logs an error if an exception occurs during parsing.
        '''
        try:
            column_names = spec_file['ColumnNames']
            offsets = spec_file['Offsets']
            data = []
            for record in input_file:
                start = 0
                output_record = dict()
                for column_name, offset in zip(column_names, offsets):
                    end = start + int(offset)
                    output_record[column_name] = record[start:end]
                    start = end
                data.append(output_record)
            return data
        except Exception as e:
            logging.error(f"Something went wrong. Plese check the error message below: \n{e}")

        