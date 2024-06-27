from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when, regexp_extract, lit, explode, regexp_replace,date_format
import re
import configparser

config = configparser.ConfigParser()
config.read(r"C:\Users\rahul\Desktop\project1234\PROJECT_260624\config_file\config.ini")

# Jar paths
jdbc_jars = [
    r"C:\Users\rahul\Desktop\project1234\ojdbc6-11.jar",
    r"C:\Users\rahul\Desktop\project1234\postgresql-42.7.3.jar",
    # r"C:\Users\rahul\Desktop\project1234\PROJECT_260624\jars\hadoop-azure-3.2.1.jar",
    # r"C:\Users\rahul\Desktop\project1234\PROJECT_260624\jars\hadoop-azure-datalake-3.2.1.jar"
]

# Join the paths with commas
jdbc_jars_str = ",".join(jdbc_jars)

# Initialize Spark session

spark = SparkSession.builder \
    .appName("WriteToAzureBlobStorage") \
    .config("spark.jars", jdbc_jars_str) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.2.1,org.apache.hadoop:hadoop-azure-datalake:3.2.1") \
    .master("local[*]") \
    .getOrCreate()

# Get Oracle configurations
oracle_host = config['oracle']['host']
oracle_port = config['oracle']['port']
oracle_schema = config['oracle']['schema']
oracle_user = config['oracle']['user']
oracle_password = config['oracle']['password']

# Get Azure configurations
storage_account_name = config['azure']['storage_account_name']
container_name = config['azure']['container_name']
storage_account_key = config['azure']['storage_account_key']

# Oracle connection parameters
host = oracle_host
port = oracle_port
schema = oracle_schema
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"


# Define Azure Blob Storage URL
blob_base_url = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"


# Azure Blob Storage configuration in Spark session
spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name), storage_account_key)


class Oracle2spark:

    def __init__(self):
        pass

    def read_data_from_oracle(self,df,enter_table_name):
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

        print(f"Number of rows of given {enter_table_name} are : - ", df.count())
        return df

    def write_to_azure_blob_storage(self,df,table_name):
        blob_url = f"{blob_base_url}/{table_name}.csv"
        return df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .format("csv") \
            .save(blob_url)



    def col_symbol_split_num_and_alphabates(self,df):
        df = df.withColumn("SYMBOL", explode(split(col("SYMBOL"), r',|and')))

        # Extract the numeric part and alphabetic part for each symbol
        df = df.withColumn("Symbol_Number", regexp_extract(col("SYMBOL"), r'(\d+)', 1)) \
            .withColumn("Symbol_Alphabets", regexp_extract(col("SYMBOL"), r'([a-zA-Z]+)', 1))
        df.drop("SYMBOL")

        return df


    def col_deductible_replace_pattern(self,df):
        df = df.withColumn("DEDUCTIBLE", regexp_replace("DEDUCTIBLE", ",", ""))

        return df


    def col_sequence_gen(self,df, *cols):

        df = df.select(*cols)

        return df

    def generate_tables_based_on_filter(self,df,enter_filter_column_name,filter_col_condtiton):

        df = (df.filter(col(enter_filter_column_name) == filter_col_condtiton))

        return df

    def process_dataset2(self,df,enter_filter_column_name,filter_col_condtiton,table_name):

        if re.match(r"TABLE_75.#([1-9]|1[0-9]|20)$",table_name):
            df = df.withColumnRenamed("Terr_code","SYMBOL") \
                    .withColumnRenamed("DEDUCTIBLE", "DEDUCTIBLE2")\
                    .withColumnRenamed("Ded_Amount","DEDUCTIBLE")

            df = df.withColumn("Terr_code",lit(1))

            df = df.withColumn("Amount", regexp_replace("Amount", "\\$", ""))

            df = self.generate_tables_based_on_filter(df, enter_filter_column_name, filter_col_condtiton)

            df = self.col_amount_split_min_max(df)

            df = self.col_symbol_split_num_and_alphabates(df)

            df = df.withColumn("DEDUCTIBLE", regexp_replace("DEDUCTIBLE", ",", ""))

            df = df.withColumn("EFFECTIVE_DATE", date_format(col("EFFECTIVE_DATE"), "yyyy-MM-dd"))

            df = df.withColumn("EXP_DATE", date_format(col("EXP_DATE"), "yyyy-MM-dd"))

            return df

        else :

            df = self.generate_tables_based_on_filter(df,enter_filter_column_name,filter_col_condtiton)

            df = self.col_amount_split_min_max(df)

            df = self.col_symbol_split_num_and_alphabates(df)

            df = df.withColumn("DEDUCTIBLE", regexp_replace("DEDUCTIBLE", ",", ""))

            df = df.withColumn("EFFECTIVE_DATE", date_format(col("EFFECTIVE_DATE"), "yyyy-MM-dd"))

            df = df.withColumn("EXP_DATE", date_format(col("EXP_DATE"), "yyyy-MM-dd"))

            return df


    def col_amount_split_min_max(self, df):

        pattern_range = r"(.*) - (.*)"
        pattern_more_than = r"More than (\d{1,3}(?:,\d{3})*)"
        pattern_less_than = r"(.*) (or Less|Or Less)"

        # pattern_range = r"(\d{1,3}(?:,\d{3})) - (\d{1,3}(?:,\d{3}))"
        # pattern_more_than = r"More than (\d{1,3}(?:,\d{3})*)"
        # pattern_less_than = r"(\d{1,3}(?:,\d{3})*) or Less"

        df = df.withColumn("Min_Amount",\
                           when(col("Amount").rlike(pattern_range), regexp_extract(col("Amount"), pattern_range, 1)) \
                           .when(col("Amount").rlike(pattern_more_than), \
                                 regexp_extract(col("Amount"), pattern_more_than, 1)) \
                           .when(col("Amount").rlike(pattern_less_than), "0") \
                           .otherwise(None))

        df = df.withColumn("Max_Amount", \
                           when(col("Amount").rlike(pattern_range), regexp_extract(col("Amount"), pattern_range, 2)) \
                           .when(col("Amount").rlike(pattern_more_than), 999999999999) \
                           .when(col("Amount").rlike(pattern_less_than), \
                                 regexp_extract(col("Amount"), pattern_less_than, 1)) \
                           .otherwise(None))

        df = df.withColumn("Min_Amount", regexp_replace(col("Min_Amount"), ",", ""))
        df = df.withColumn("Max_Amount", regexp_replace(col("Max_Amount"), ",", ""))

        return df


    # def col_amount_split_min_max(self, df):
    #     df = (df.withColumn("Amount", regexp_replace("Amount", "$", ""))
    #           .withColumn("Min_Amount",
    #                       when(col("AMOUNT").rlike("More than"),
    #                            regexp_extract(col("AMOUNT"), r"More than (\d+,\d+)", 1))
    #                       .when((col("AMOUNT").rlike("or Less")) | (col("AMOUNT").rlike("Or Less")), lit(0))
    #                       .when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 1))
    #                       .otherwise(lit(None)))  # Add otherwise() to handle unmatched cases
    #           .withColumn("Max_Amount",
    #                       when(col("AMOUNT").rlike("More than"), lit(9999999999))
    #                       .when((col("AMOUNT").rlike("or Less")) | (col("AMOUNT").rlike("Or Less")),
    #                             regexp_extract(col("AMOUNT"), r"(\d+,\d+) or Less", 1))
    #                       .when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 2))
    #                       .otherwise(lit(None))))  # Add otherwise() to handle unmatched cases
    #     df = df.drop("AMOUNT")
    #     # Show the result
    #     return df

    #
    #
    # def process_dataset(self, df,enter_filter_column_name,filter_col_condtiton):
    #
    #     df = (df.filter(col(enter_filter_column_name) == filter_col_condtiton))
    #
    #     df = (df.withColumn("Amount", regexp_replace("Amount", "$", ""))\
    #           .withColumn("Min_Amount",
    #                        when(col("AMOUNT").rlike("More than"),
    #                             regexp_extract(col("AMOUNT"), r"More than (\d+,\d+)", 1))
    #                        .when(col("AMOUNT").rlike("or Less"| "Or Less"), lit(0))
    #                        .when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 1))) \
    #         .withColumn("Max_Amount",
    #                     when(col("AMOUNT").rlike("More than"), lit(9999999999))
    #                     .when(col("AMOUNT").rlike("or Less"| "Or Less"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) or Less", 1))
    #                     .when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 2))))
    #     df = df.drop("AMOUNT")
    #
    #     df = df.withColumn("DEDUCTIBLE", regexp_replace("DEDUCTIBLE", ",", "")) \
    #         .withColumn("Max_Amount", regexp_replace("Max_Amount", ",", "")) \
    #         .withColumn("Min_Amount", regexp_replace("Min_Amount", ",", ""))
    #
    #     df = df.withColumn("SYMBOL", explode(split(col("SYMBOL"), r',|and')))
    #
    #     df = df.withColumn("Symbol_Number", regexp_extract(col("SYMBOL"), r'(\d+)', 1)) \
    #         .withColumn("Symbol_Alphabets", regexp_extract(col("SYMBOL"), r'([a-zA-Z]+)', 1))
    #     df = df.drop("SYMBOL")
    #
    #     df = df.withColumn("DEDUCTIBLE", regexp_replace("DEDUCTIBLE", ",", ""))
    #
    #     df = df.withColumn("EFFECTIVE_DATE", date_format(col("EFFECTIVE_DATE"), "yyyy-MM-dd"))
    #
    #     df = df.withColumn("EXP_DATE", date_format(col("EXP_DATE"), "yyyy-MM-dd"))
    #
    #     return df

    # def col_amount_split_min_max(self,df):
    #     df = df.withColumn("Amount", regexp_replace("Amount", "$", ""))
    #
    #     df = df.withColumn("Min_Amount",
    #                       when(col("AMOUNT").rlike("More than"),
    #                        regexp_extract(col("AMOUNT"), r"More than (\d+,\d+)", 1))\
    #                       .when((col("AMOUNT").rlike("or Less")) | (col("AMOUNT").rlike("or Less"))), lit(0))\
    #                       .when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 1)) \
    #                       .otherwise(lit(None))\
    #           .withColumn("Max_Amount",
    #                       when(col("AMOUNT").rlike("More than"), lit(9999999999))
    #                       .when((col("AMOUNT").rlike("or Less")) | (col("AMOUNT").rlike("Or Less") ),
    #                         regexp_extract(col("AMOUNT"), r"(\d+,\d+) or Less", 1))
    #                       .when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 2))
    #                       .otherwise(lit(None)))
    #
    #     df = df.drop("AMOUNT")
    #     # Show the result
    #     return df.show()



