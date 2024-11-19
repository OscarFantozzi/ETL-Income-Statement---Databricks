# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

print("="*100)
print('STEP 2: CLEANING DATA')

# define the paths for Bronze and Silver layers
bronze_path = "dbfs:/user/hive/warehouse/bronze/"
silver_path = "dbfs:/user/hive/warehouse/silver/"

# list all files in the bronze directory
files = dbutils.fs.ls(bronze_path)

# dictionary to store the cleaned DataFrames
tables_dict = {}

# loop through the files and load them dynamically from Bronze
for file in files:
    # extract the file name (removing "/" sign)
    file_name = file.name.split('/')[0]

    # load the file dynamically from the Bronze layer
    df = spark.read.format("delta").load(file.path)
    
    # check the file name and apply appropriate transformation
    if file_name == 'dEstruturaDRE':
        df_clean = df.dropna() \
            .withColumn("id", F.col("id").cast(StringType())) \
            .withColumn("index", F.col("index").cast(IntegerType())) \
            .withColumn("subtotal", F.col("subtotal").cast(IntegerType())) \
            .withColumnRenamed("contaGerencial", "ManagementAccount") \
            .withColumnRenamed("empresa", "Branch") \
            .withColumnRenamed("subtotal", "Subtotal") \
            .withColumnRenamed("index", "Index") \
            .withColumn("Amount", F.lit(None).cast(FloatType()))  # Para garantir que Amount existe

        tables_dict[file_name] = df_clean
        print(f"'{file_name}' has been cleaned. It has {df_clean.count()} rows.")
    
    elif file_name == 'dPlanoConta':
        df_clean = df.dropna() \
            .withColumn("mascaraDRE_id", F.col("mascaraDRE_id").cast(StringType())) \
            .withColumnRenamed("descricaoN1", "DescriptionLevel1") \
            .withColumnRenamed("descricaoN2", "DescriptionLevel2") \
            .withColumnRenamed("detalharN2", "DetailLevel2?") \
            .withColumnRenamed("mascaraDRE_id", "IncomeStatementTemplate_id") \
            .withColumnRenamed("tipoLancamento", "EntryType") \
            .withColumnRenamed("index", "Index") \
            .withColumn("Amount", F.lit(None).cast(FloatType()))  # Garantindo Amount

        tables_dict[file_name] = df_clean
        print(f"'{file_name}' has been cleaned. It has {df_clean.count()} rows.")
    
    elif file_name == 'fOrcamento':
            df_clean = (df.withColumn("competencia_data", F.to_date(F.col("competencia_data"), "d/M/yyyy"))
                        .withColumn("valor", F.col("valor").cast("string"))
                        .withColumn("valor", F.regexp_replace("valor", r'\.', ''))  
                        .withColumn("valor", F.regexp_replace("valor", r'(?<=\d)(?=(\d{2})$)', '.'))  
                        .withColumn("valor", 
                        F.when(F.col("valor").contains('.'), F.col("valor"))
                        .otherwise(F.concat(F.col("valor"), F.lit(".00"))))  
                        .withColumnRenamed("competencia_data", "AccrualDate")
                        .withColumnRenamed("planoContas_id", "ChartOfAccounts_id")
                        .withColumnRenamed("valor", "Amount")
                        .withColumn("Amount", F.col("Amount").cast(DecimalType(precision=20, scale=2))) )

            tables_dict[file_name] = df_clean
            print(f"'{file_name}' has been cleaned. It has {df_clean.count()} rows.")

    elif file_name == 'fPrevisao':
        df_clean = (df.withColumn("competencia_data", F.to_date(F.col("competencia_data"), "d/M/yyyy"))
                    .withColumn("valor", F.col("valor").cast("string"))
                    .withColumn("valor", F.regexp_replace("valor", r'\.', ''))  
                    .withColumn("valor", F.regexp_replace("valor", r'(?<=\d)(?=(\d{2})$)', '.'))  
                    .withColumn("valor", 
                    F.when(F.col("valor").contains('.'), F.col("valor"))
                    .otherwise(F.concat(F.col("valor"), F.lit(".00"))))  
                    .withColumnRenamed("competencia_data", "AccrualDate")
                    .withColumnRenamed("planoContas_id", "ChartOfAccounts_id")
                    .withColumnRenamed("valor", "Amount")
                    .withColumn("Amount", F.col("Amount").cast(DecimalType(precision=20, scale=2))) )

        silver_table_path = f"{silver_path}{file_name}"

            # Saving to Delta with mergeSchema to handle schema conflicts
        df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_table_path)

        # Register the table in the Silver layer
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS silver.{file_name}
            USING DELTA
            LOCATION '{silver_table_path}'
        """)

        print(f"Processed and registered in Silver: {file_name}")
    
        tables_dict[file_name] = df_clean
        print(f"'{file_name}' has been cleaned. It has {df_clean.count()} rows.")

    elif file_name.startswith('fLancamento'):
        df_clean = ( df.withColumn("competencia_data", F.to_date(F.col("competencia_data"), "d/M/yyyy")) \
            .withColumn("valor", F.col("valor").cast("string")) \
            .withColumn("valor", F.regexp_replace("valor", r'\.', '')) \
            .withColumn("valor", F.regexp_replace("valor", r'(?<=\d)(?=(\d{2})$)', '.')) \
            .withColumn("valor", 
                            F.when(F.col("valor").contains('.'), F.col("valor"))
                            .otherwise(F.concat(F.col("valor"), F.lit(".00")))) \
            .withColumnRenamed("competencia_data", "AccrualDate") \
            .withColumnRenamed("planoContas_id", "ChartOfAccounts_id") \
            .withColumnRenamed("valor", "Amount") \
            .withColumn("Amount", F.col("Amount").cast(DecimalType(precision=20, scale=2))) )

        if 'fLancamentos' in tables_dict:
            tables_dict['fLancamentos'] = tables_dict['fLancamentos'].union(df_clean)
        else:
            tables_dict['fLancamentos'] = df_clean

        print(f"Processed: {file_name}")


# after loading and cleaning, save the cleaned data to the Silver Layer
for table_name, df_clean in tables_dict.items():

    silver_table_path = f"{silver_path}{table_name}"

    # Saving to Delta with mergeSchema to handle schema conflicts
    df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_table_path)

    # Register the table in the Silver layer
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver.{table_name}
        USING DELTA
        LOCATION '{silver_table_path}'
    """)

    print(f"Processed and registered in Silver: {table_name}")


# COMMAND ----------


