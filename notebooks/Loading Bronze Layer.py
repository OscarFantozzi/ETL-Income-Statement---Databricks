# Databricks notebook source
# MAGIC %md
# MAGIC ### Load Bronze Layer using Delta

# COMMAND ----------

# path landing zone
path_lz = 'dbfs:/FileStore/landing_zone/'

# list files in the landing_zone directory
files = dbutils.fs.ls(path_lz)

for file in files:
    # check if the file has .csv
    if file.name.endswith(".csv"):
        # read file
        df = spark.read.format('csv').option("header", "true").load(file.path)

        # table name removing extension
        table_name = file.name.split('.')[0]

        # defining bronze path layer
        bronze_path = f"dbfs:/user/hive/warehouse/bronze/{table_name}"

        # saving as delta format
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(bronze_path)

        # creating table if not exists
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze.{table_name}
            USING DELTA
            LOCATION '{bronze_path}'
        """)

        print(f"Processed and registered: {file.name}")
