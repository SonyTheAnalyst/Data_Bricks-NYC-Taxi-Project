# Databricks notebook source
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import TimestampType, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

df = spark.read.format('csv').option('header', True).load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")
display(df)

# COMMAND ----------

df= df.select(
                col("LocationID").cast(IntegerType()).alias('location_id'),
                col('Borough').alias('borough'),
                col('Zone').alias('zone'),
                col('service_zone'),
                current_timestamp().alias('effective_date'),
                lit(None).cast(TimestampType()).alias('end_date') #LIT CREATE A LITTERAL NULL VALUES

)


# COMMAND ----------

display(df)
df.show()

# COMMAND ----------

# Fixed point-in-time used to "close" any changed active records
# Using a Python timestamp ensures the exact same value is written and can be referenced if needed
end_timestamp = datetime.now()

# Load the SCD2 Delta table
dt = DeltaTable.forName(spark, "nyctaxi.`02_silver`.taxi_zone_lookup")

# COMMAND ----------

###df.write.mode('overwrite').format('delta').saveAsTable('nyctaxi.`02_silver`.taxi_zone_lookup')

# COMMAND ----------

spark.read.table('nyctaxi.`02_silver`.taxi_zone_lookup').display()