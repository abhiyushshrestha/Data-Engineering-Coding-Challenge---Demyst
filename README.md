# Data-Engineering-Coding-Challenge---Demyst

# Problem Overview

## Problem 1: Parsing a Fixed-Width File

- **Objective**: Generate a fixed-width file based on a provided specification (where the offsets represent the length of each field). Then, implement a parser that can convert this fixed-width file into a delimited format, such as CSV.
- **Constraints**: Do not use external Python libraries like pandas for parsing. However, the standard library can be used for writing the CSV file if needed.
- **Language Options**: The solution can be implemented in Python or Scala.
- **Delivery**: Source code should be provided via GitHub or Bitbucket.
- **Bonus**: Extra points if a Docker container (Dockerfile) is provided to easily run the code.
- **Important Consideration**: Ensure the handling of file encoding.

## Problem 2: Data Processing and Anonymization

- **Objective**: Generate a CSV file containing columns for `first_name`, `last_name`, `address`, and `date_of_birth`. Process this file to anonymize the data, specifically the `first_name`, `last_name`, and `address` fields.
- **Challenge**: Ensure that the solution works efficiently on a 2GB CSV file, and demonstrate scalability for even larger datasets.
- **Hint**: Consider using a distributed computing platform to handle larger datasets effectively.

This README provides a detailed overview of the tasks, outlining the goals, constraints, and additional challenges for both problems.


## Docker Setup Instructions

### Building the Docker Image

To build the Docker image for the project, use the following command:

```bash
docker build -t problem_1 .

```bash
docker run -t problem_1