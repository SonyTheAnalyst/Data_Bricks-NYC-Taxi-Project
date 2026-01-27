# Databricks notebook source
spark.sql(
    "CREATE CATALOG IF NOT EXISTS nyctaxi MANAGED LOCATION 'abfss://unity-catalog-storage@dbstorage2bqmoi3bc7r6u.dfs.core.windows.net/7405616564282883' "
)

# COMMAND ----------

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS nyctaxi.00_landing"
          )

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS nyctaxi.01_bronze"
          )

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS nyctaxi.02_silver"
          )

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS nyctaxi.03_gold"
          )

# COMMAND ----------

spark.sql(
    "CREATE VOLUME IF NOT EXISTS nyctaxi.00_landing.data_sources"
)