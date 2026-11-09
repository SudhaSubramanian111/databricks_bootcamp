import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name = 'customers_silver_view'
)
#creating the silver view based on the bronze table created
def customers_silver_view():
    df_cust = spark.readStream.table("customer_bronze")
    df_cust=df_cust.withColumn('name',upper(col("name")))
    df_cust=df_cust.withColumn('domains',split(col("email"),'@').getItem(1))
    df_cust = df_cust.withColumn("processDate",current_timestamp())
    return df_cust

#creating empty staging table - customer silver table
dlt.create_streaming_table(name ="customer_silver")

#it will upsert the data
dlt.create_auto_cdc_flow(
    source = "customers_silver_view",
    target = "customer_silver",
    keys= ['customer_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=1

)