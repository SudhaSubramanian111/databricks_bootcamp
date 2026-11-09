import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = 'products_silver_view'
)
#creating the silver view based on the bronze table created
def products_silver_view():
    df_prd = spark.readStream.table("products_bronze")
    df_prd = df_prd.withColumn("processDate",current_timestamp())
    return df_prd

#creating empty staging table
dlt.create_streaming_table(name ="products_silver")

#it will upsert the data
dlt.create_auto_cdc_flow(
    source = "products_silver_view",
    target = "products_silver",
    keys= ['product_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=1
)