# Problem Overview

## Problem 2: Data Processing and Anonymization

- **Objective**: Generate a CSV file containing columns for `first_name`, `last_name`, `address`, and `date_of_birth`. Process this file to anonymize the data, specifically the `first_name`, `last_name`, and `address` fields.

## Usage Instructions

### Running the Python Script

To execute the Python script for data processing, use the following commands:

1. **With data generation:**

    ```bash
    python src/main.py -i 'data/input_file/generated_file.csv' -o 'data/output_file/anonymize_data' -g 'data/input_file/generated_file.csv' -gr True -n 10 -s xs
    ```

    **Explanation:**
    - `-i 'data/input_file/generated_file.csv'`: Path to the input CSV file.
    - `-o 'data/output_file/anonymize_data'`: Path to the output directory where anonymized data will be saved.
    - `-g 'data/input_file/generated_file.csv'`: Path to the generated CSV file for dummy data.
    - `-gr True`: Flag to indicate that dummy records should be generated.
    - `-n 10`: Number of dummy records to generate.
    - `-s xs`: Size of the dataset.

2. **Without data generation:**

    ```bash
    python src/main.py -i 'data/input_file/generated_file.csv' -o 'data/output_file/anonymize_data' -s xs
    ```

    **Explanation:**
    - `-i 'data/input_file/generated_file.csv'`: Path to the input CSV file.
    - `-o 'data/output_file/anonymize_data'`: Path to the output directory where anonymized data will be saved.
    - `-s xs`: Size of the dataset.

### Docker Setup

#### Building the Docker Image

To build the Docker image for the project, use the following command:

```bash
docker build -t problem_2 .
```

```bash
docker run -t problem_2
```