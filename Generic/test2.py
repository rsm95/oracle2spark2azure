from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when, regexp_extract, lit, explode, regexp_replace, trim

import configparser

config = configparser.ConfigParser()
config.read('config.ini')


# Jar paths
jdbc_jars = [
    r"C:\Users\rahul\Desktop\project1234\ojdbc6-11.jar",
    r"C:\Users\rahul\Desktop\project1234\postgresql-42.7.3.jar"
]

# Join the paths with commas
jdbc_jars_str = ",".join(jdbc_jars)

# Initialize Spark session

spark = SparkSession.builder \
    .appName("WriteToAzureBlobStorage") \
    .config("spark.jars", jdbc_jars_str) \
    .master("local[*]") \
    .getOrCreate()


# Get Oracle configurations
oracle_host = config['oracle']['host']
oracle_port = config['oracle']['port']
oracle_schema = config['oracle']['schema']
oracle_user = config['oracle']['user']
oracle_password = config['oracle']['password']

# Get Azure configurations
azure_storage_account_name = config['azure']['storage_account_name']
azure_container_name = config['azure']['container_name']
azure_storage_account_key = config['azure']['storage_account_key']


# Oracle connection parameters
host = oracle_host
port = oracle_port
schema = oracle_schema
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"

def read_data_from_oracle(df, enter_table_name):
    query = f"SELECT * FROM {enter_table_name}"
    df = spark.read.format("jdbc") \
        .option("url", URL) \
        .option("query", query) \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

    print("#########################################################")
    print(f"{enter_table_name}")

    df.printSchema()

    print("Number of rows of given TABLE73 are : - ", df.count())
    return df.show()


def col_amount_split_min_max(df):

    df= df.withColumn("Min_Amount",
                     when(col("AMOUNT").rlike("More than"), regexp_extract(col("AMOUNT"), r"More than (\d+,\d+)", 1)).
                     when(col("AMOUNT").rlike("or Less"), lit(0)).
                     when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 1)))\
        .withColumn("Max_Amount",
                     when(col("AMOUNT").rlike("More than"), lit(9999999999)).
                     when(col("AMOUNT").rlike("or Less"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) or Less", 1)).
                     when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 2)))
    df.drop("AMOUNT")
    # Show the result
    return df.show()


def col_symbol_split_num_and_alphabates(df):

    df = df.withColumn("SYMBOL", explode(split(col("SYMBOL"), r',|and')))

    # Extract the numeric part and alphabetic part for each symbol
    df = df.withColumn("Symbol_Number", regexp_extract(col("SYMBOL"), r'(\d+)', 1)) \
        .withColumn("Symbol_Alphabets", regexp_extract(col("SYMBOL"), r'([a-zA-Z]+)', 1))
    df.drop("SYMBOL")

    return df.show()

def col_deductible_replace_pattern(df):

    df = df.withColumn("DEDUCTIBLE", regexp_replace("DEDUCTIBLE", ",", ""))

    return df.show()

print("Number of rows of given transformed TABLE73_1 are : - ", df.count())


def col_sequence_gen(df,*cols):
    df = df.select(*cols)
    return df.show()

