# Databricks notebook source
#LOAD THE BRONZE DATA INTO A SPARK DATAFRAME FROM THE CATALOG
df = spark.read.format('parquet').load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")
df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df= df.withColumn(
    'processed_timestamp',
    current_timestamp()
)
df.select('processed_timestamp').show()

# COMMAND ----------

df.write.mode('overwrite').saveAsTable("nyctaxi.`01_bronze`.yellow_trips_raw")
spark.read.table("nyctaxi.`01_bronze`.yellow_trips_raw").display()

# COMMAND ----------

#spark.read.parquet("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/2025-06/yellow_tripdata_2025-06.parquet").display()