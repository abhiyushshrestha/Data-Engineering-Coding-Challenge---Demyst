# from pyspark.sql import SparkSession
# from pyspark.sql.functions import sha2, concat_ws, lit

# # Initialize Spark session
# spark = SparkSession.builder.appName("Anonymize Data").getOrCreate()

# # Example DataFrame
# data = [
#     ('Alice', 'alice@example.com', '123-456-7890'),
#     ('Bob', 'bob@example.com', '987-654-3210'),
#     ('Charlie', 'charlie@example.com', '555-555-5555')
# ]

# columns = ['Name', 'Email', 'Phone']
# df = spark.createDataFrame(data, columns)

# # Anonymize the 'Name' and 'Email' columns by hashing
# df = df.withColumn('Name', sha2(df['Name'], 256)) \
#        .withColumn('Email', sha2(df['Email'], 256)) \
#        .withColumn('Phone', sha2(df['Phone'], 256))

# # Drop the original columns if needed
# # df = df.drop('Name', 'Email')

# df.show(truncate=False)

from faker import Faker
fake = Faker()

for i in range(0, 4):
    # print(fake.first_name())
    # print(fake.last_name())
    # print(fake.basic_phone_number())
    print(fake.street_address())

# data = [
#             {'first_name': 'Abhiyush', 'last_name': 'Shrestha', 'address': 'mel'},
#             {'first_name': 'Abhiyush1', 'last_name': 'Shrestha1', 'address': 'mel'},
#             {'first_name': 'Abhiyush2', 'last_name': 'Shrestha2', 'address': 'mel'},
#             {'first_name': 'Abhiyush3', 'last_name': 'Shrestha3', 'address': 'mel'},
#         ]

# print(list(data[0].keys()))