from utils.cmutils import Oracle2spark
from pyspark.sql.functions import col

oracle = Oracle2spark()

if __name__ =="__main__":

    enter_table_name_from_oracledb = "TABLE73"

    df = oracle.read_data_from_oracle("df", enter_table_name_from_oracledb)
    df.show()

    a = df.select("TABLE_NUMBER").distinct().orderBy("TABLE_NUMBER").count()

    filterd_col_condtions = []

    for i in range(0,a):
        b = df.select("TABLE_NUMBER").distinct().orderBy("TABLE_NUMBER").collect()[i][0]
        filterd_col_condtions.append(b)

    tables = {}

    # for i in filterd_col_condtions:
    #     table_name = f"TABLE_{i}"
    #     tables[table_name] = oracle.generate_tables_based_on_filter(df, "TABLE_NUMBER", f"{i}")
    #     print(f"############ {table_name} ############")
    #     tables[table_name].show()
    #     print(f"Number of rows of given {table_name} are : - ", tables[table_name].count())
    #     print(" ")
    #     print("##################################################################")


    for i in filterd_col_condtions:
        table_name = f"TABLE_{i}"
        # tables[table_name] = oracle.process_dataset(df, "TABLE_NUMBER", f"{i}")
        tables[table_name] = oracle.process_dataset2(df, "TABLE_NUMBER", f"{i}",table_name)

    for i in filterd_col_condtions:
        table_name = f"TABLE_{i}"
        final_output = oracle.col_sequence_gen(tables[table_name], "STATE_CODE","TABLE_NUMBER","EFFECTIVE_DATE","EXP_DATE","TERR_CODE","Min_Amount","Max_Amount","DEDUCTIBLE","Symbol_Number","Symbol_Alphabets","FACTOR")
        oracle.write_to_azure_blob_storage(final_output,table_name)
        print(f"############ {table_name} ############")
        print(final_output.show(50))
        print(f"Number of rows of given transformed {table_name} are : - ",final_output.count())
        print(f"{table_name} written to Azure Blob Storage successfully.")


    #
    # tables["TABLE_73.#1"].filter(col("Min_Amount")==50001).show()