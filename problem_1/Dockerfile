FROM python:3.11

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container
COPY . .

# Command to run the application
CMD ["python", "./src/main.py", "-s", "./data/spec_file/spec.json",  "-i", "./data/input_file/input.txt",  "-o", "./data/output_file/output.csv"]