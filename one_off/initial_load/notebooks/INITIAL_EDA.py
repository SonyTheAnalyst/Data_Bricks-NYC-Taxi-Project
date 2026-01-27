# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.table('nyctaxi.`02_silver`.yelllow_trips_enriched')

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ...................................

# COMMAND ----------

df.groupBy(
    "vendor"
).agg(
        round(
            sum('total_amount')
            ).alias('total_revenue'),
).display()

# COMMAND ----------

spark.read.table('nyctaxi.`02_silver`.yelllow_trips_enriched')

# COMMAND ----------

df.groupBy('pu_borough').agg(
    count('*').alias('number_of_trips')
).display()

# COMMAND ----------

df.groupBy(concat('pu_borough', lit(' -> '), 'do_borough').alias('journey')).agg(
    count('*').alias('number_of_trips')
).display()

# COMMAND ----------

df2= spark.read.table('nyctaxi.`03_gold`.daily_trip_summary')
df2.display()

# COMMAND ----------



# COMMAND ----------

