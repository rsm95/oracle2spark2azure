from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,when,regexp_extract,lit,explode,regexp_replace,trim

#Jar paths
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

# Oracle connection parameters
host = "localhost"
port = "1521"
schema = "XE"
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"

# Loading data from Oracle into DataFrame
def read_data_from_oracle(df,enter_table_name):

    query = f"SELECT * FROM {enter_table_name}"
    df = spark.read.format("jdbc") \
        .option("url", URL) \
        .option("query", query) \
        .option("user", "sys as sysdba") \
        .option("password", "1995") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

    df.printSchema()

    print("Number of rows of given TABLE73 are : - ",df.count())
    df.show()

print("#########################################################")
print("TABLE73_1")

query1 = "SELECT * FROM TABLE73_1"
df1 = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("query", query1) \
    .option("user", "sys as sysdba") \
    .option("password", "1995") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

df1.printSchema()

print("Number of rows of given TABLE73_1 are : - ",df1.count())
df1.show()

print("#########################################################")
print("TABLE73_2")

query2 = "SELECT * FROM TABLE73_2"
df2 = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("query", query2) \
    .option("user", "sys as sysdba") \
    .option("password", "1995") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

df2.printSchema()

print("Number of rows of given TABLE73_2 are : - ",df2.count())
df2.show()

print("#########################################################")
print("TABLE73_3")

query3 = "SELECT * FROM TABLE73_3"
df3 = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("query", query3) \
    .option("user", "sys as sysdba") \
    .option("password", "1995") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

df3.printSchema()

print("Number of rows of given TABLE73_3 are : - ",df3.count())
df3.show()

pattern_more_than = r"^More than (\d+)$"
pattern_range = r"^(\d+) - (\d+)$"
pattern_or_less = r"^(\d+) or less$"


df4 = df1.withColumn("Min_Amount",
                   when(col("AMOUNT").rlike("More than"), regexp_extract(col("AMOUNT"), r"More than (\d+,\d+)", 1)).
                   when(col("AMOUNT").rlike("or Less"), lit(0)).
                   when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 1)))

df5 = df4.withColumn("Max_Amount",
                   when(col("AMOUNT").rlike("More than"), lit(9999999999)).
                   when(col("AMOUNT").rlike("or Less"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) or Less", 1)).
                   when(col("AMOUNT").rlike("-"), regexp_extract(col("AMOUNT"), r"(\d+,\d+) - (\d+,\d+)", 2)))

# Show the result
df5.show(truncate=False)
#
# # Split each row by "," and explode to separate rows
# df6 = df5.withColumn(df.SYMBOL, explode(split(df["SYMBOL"], ",")))
#
# # Split each symbol pair into separate columns
# df6 = df6.withColumn("Symbol_number", split(df["SYMBOL"], " ")[0])
# df6 = df6.withColumn("Symbol_alphabates", split(df["SYMBOL"], " ")[1])
#
# # Select and show the final transformed DataFrame
# df6.show()
#


df6 = df5.withColumn("SYMBOL", explode(split(col("SYMBOL"), r',|and')))

# Extract the numeric part and alphabetic part for each symbol
df6 = df6.withColumn("Symbol_Number", regexp_extract(col("SYMBOL"), r'(\d+)', 1)) \
       .withColumn("Symbol_Alphabets", regexp_extract(col("SYMBOL"), r'([a-zA-Z]+)', 1))

df6.show()

df7 = df6.withColumn("DEDUCTIBLE",regexp_replace("DEDUCTIBLE",",",""))
df7 = df7.drop("AMOUNT","SYMBOL")


print("Number of rows of given transformed TABLE73_1 are : - ",df7.count())

df7 = df7.select("STATE_CODE","TABLE_NUMBER","EFFECTIVE_DATE","EXP_DATE","TERR_CODE","Min_Amount","Max_Amount","DEDUCTIBLE","Symbol_Number","Symbol_Alphabets","FACTOR")
df7.show()
#
# df4 = df3.withColumn("min_amount",
#                    when(col("amount").contains("More than"), split(col("amount"), "More than ")[1].cast("integer"))
#                    .when(col("amount").contains("-"), split(col("amount"), "-")[0].cast("integer"))
#                     .when(col("amount").contains("or less"), 0)
#                    .when(col("amount").contains("or less"), split(col("amount"), " or less")[0].cast("integer"))
#                    .otherwise(col("amount").cast("integer")))
#
# df4 = df4.withColumn("max_amount",
#                    when(col("amount").contains("More than"), 99999999)
#                    .when(col("amount").contains("-"), split(col("amount"), "-")[1].cast("integer"))
#                    .when(col("amount").contains("or less"), 0)
#                    .otherwise(col("amount").cast("integer")))

# Drop the original amount column
# df4 = df3.drop("amount")

# Show the final DataFrame
# df4.show()

# Drop the original amount column
# df4 = df4.drop("amount")

# Show the final DataFrame
# df4.show()
#
# pattern_range = r"(\d{1,3}(?:,\d{3})) - (\d{1,3}(?:,\d{3}))"
# pattern_more_than = r"More than (\d{1,3}(?:,\d{3})*)"
# pattern_less_than = r"(\d{1,3}(?:,\d{3})*) or Less"
#
# df4 = df3.withColumn("Min_Amount",
#                    when(col("Amount").rlike(pattern_range), regexp_extract(col("Amount"), pattern_range, 1))\
#                    .when(col("Amount").rlike(pattern_more_than),
#                          regexp_extract(col("Amount"), pattern_more_than, 1))\
#                    .when(col("Amount").rlike(pattern_less_than), "0")\
#                    .otherwise(None))
#
# df.show()
#
# df4 = df4.withColumn("Max_Amount",
#                    when(col("Amount").rlike(pattern_range), regexp_extract(col("Amount"), pattern_range, 2))\
#                    .when(col("Amount").rlike(pattern_more_than), 999999999999)\
#                    .when(col("Amount").rlike(pattern_less_than),
#                          regexp_extract(col("Amount"), pattern_less_than, 1))\
#                    .otherwise(None))
#
# df4.show()

#
# df1 = oracle.process_dataset(df,"TABLE_NUMBER","73.#1")
# df2 = oracle.process_dataset(df,"TABLE_NUMBER","73.#2")
# df3 = oracle.process_dataset(df,"TABLE_NUMBER","73.#3")
#
# final_output = oracle.col_sequence_gen(df1, "STATE_CODE","TABLE_NUMBER","EFFECTIVE_DATE","EXP_DATE","TERR_CODE","Min_Amount","Max_Amount","DEDUCTIBLE","Symbol_Number","Symbol_Alphabets","FACTOR")
#
# for i in final_output:
#     i.show()
#     print(i.count())