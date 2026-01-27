# Databricks notebook source
# Import the current_timestamp function from PySpark.
# This is typically used to add a column containing the current system timestamp
# when transforming or creating DataFrames.
from pyspark.sql.functions import current_timestamp

# Import relativedelta from the dateutil library.
# relativedelta allows flexible date arithmetic (e.g., add/subtract months, years),
# which is more accurate than timedelta for calendar-aware operations.
from dateutil.relativedelta import relativedelta



# COMMAND ----------

# DBTITLE 1,Cell 2
from datetime import date

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

# Read all Parquet files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD THE BRONZE DATA INTO A SPARK DATAFRAME FROM THE CATALOG
# MAGIC df = spark.read.format('parquet').load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")
# MAGIC df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df= df.withColumn(
    'processed_timestamp',
    current_timestamp()
)
df.select('processed_timestamp').show()

# COMMAND ----------

df.write.mode('append').saveAsTable("nyctaxi.`01_bronze`.yellow_trips_raw")
spark.read.table("nyctaxi.`01_bronze`.yellow_trips_raw").display()

# COMMAND ----------

#spark.read.parquet("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/2025-06/yellow_tripdata_2025-06.parquet").display()