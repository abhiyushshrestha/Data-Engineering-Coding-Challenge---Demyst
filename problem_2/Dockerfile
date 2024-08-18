FROM python:3.12

# Install dependencies
RUN apt-get update && \
    apt-get install -y default-jdk && \
    # apt-get install -y openjdk-7-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Verify Java installation
RUN java -version

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container
COPY . .

# Run pip to install all the required libraries
RUN pip install -r ./requirements.txt

# Command to run the application
CMD ["python", "./src/main.py", "-i", "./data/input_file/generated_file.csv",  "-o", "./data/output_file/anonymize_data",  "-g", "./data/input_file/generated_file.csv", "-gr", "True", "-n", "10", "-s", "xs"]

# CMD ["python", "./src/main.py", "-i", ".data/input_file/generated_file.csv",  "-o", "./data/output_file/anonymize_data", "-s", "xs"]
