# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round

# Load the enriched trip dataset
df = spark.read.table("nyctaxi.`02_silver`.yelllow_trips_enriched")
df.display()

# COMMAND ----------

from pyspark.sql.functions import *
# Aggregate trip data by pickup date with key metrics

# group records by calendar date
df = df.groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).agg(
            count("*").alias("total_trips"),                             # total number of trips per day
            round(avg("passenger_count"), 1).alias("average_passengers"), # average passengers per trip
            round(avg("trip_distance"), 1).alias("average_distance"),     # average trip distance (miles)
            round(avg("fare_amount"), 2).alias("average_fare_per_trip"),   # average fare per trip ($)
            max("fare_amount").alias("max_fare"),                         # highest single-trip fare
            min("fare_amount").alias("min_fare"),                         # lowest single-trip fare
            round(sum("total_amount"), 2).alias("total_revenue")          # total revenue for the day ($)
        )


df.display()

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyctaxi.`03_gold`.daily_trip_summary')

#==========================================================================================#
#==========================================================================================#

spark.read.table('nyctaxi.`03_gold`.daily_trip_summary').show()
