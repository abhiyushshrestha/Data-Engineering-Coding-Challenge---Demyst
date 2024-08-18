# Problem Overview

## Problem 1: Parsing a Fixed-Width File

- **Objective**: Generate a fixed-width file based on a provided specification (where the offsets represent the length of each field). Then, implement a parser that can convert this fixed-width file into a delimited format, such as CSV.

## Usage Instructions

### Running the Python Script

To execute the Python script, use the following command:

```bash
python src/main.py -s data/spec_file/spec.json -i data/input_file/input.txt -o data/output_file/output.csv
```

## Docker Setup Instructions

### Building the Docker Image

To build the Docker image for the project, use the following command:

```bash
docker build -t problem_1 .
```

```bash
docker run -t problem_1
```