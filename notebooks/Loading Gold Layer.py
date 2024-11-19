# Databricks notebook source
# define the paths for silver and gold layers
silver_path = "dbfs:/user/hive/warehouse/silver/"
gold_path = "dbfs:/user/hive/warehouse/gold/"

# list all files in the silver layer
silver_files = dbutils.fs.ls(silver_path)

# dictionary to store processed tables
tables_dict = {}

# loop through the files in the silver layer and move them to the gold layer
for file in silver_files:
    # extract the file name (removing the extension)
    table_name = file.name.split('/')[0]
    
    # load the file from the silver layer
    df_silver = spark.read.format("delta").load(file.path)
    
    # define the path for the gold layer
    gold_table_path = f"{gold_path}{table_name}"
    
    # write the data to the gold layer
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_table_path)


    # register the table in the gold layer if it does not exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.{table_name}
        USING DELTA
        LOCATION '{gold_table_path}'
    """)
    


