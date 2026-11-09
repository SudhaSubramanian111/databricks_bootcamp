import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name = 'stores_silver_view'
)
#creating the silver view
def stores_silver_view():
    df_store = spark.readStream.table("store_bronze")
    df_store = df_store.withColumn("store_name", regexp_replace(col("store_name"), "_", ""))
    df_store = df_store.withColumn("processDate",current_timestamp())
    return df_store

#creating empty staging table
dlt.create_streaming_table(name ="stores_silver")

#it will upsert the data
dlt.create_auto_cdc_flow(
    source = "stores_silver_view",
    target = "stores_silver",
    keys= ['store_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=1

)