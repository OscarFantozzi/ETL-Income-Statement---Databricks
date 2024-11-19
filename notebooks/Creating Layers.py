# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Landing Zone

# COMMAND ----------

# creating landing zone
dbutils.fs.mkdirs( 'FileStore/landing_zone' )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Bronze Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.bronze
# MAGIC LOCATION 'dbfs/FileStore/bronze/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.silver
# MAGIC LOCATION 'dbfs/FileStore/silver/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.gold
# MAGIC LOCATION 'dbfs/FileStore/gold/';

# COMMAND ----------


