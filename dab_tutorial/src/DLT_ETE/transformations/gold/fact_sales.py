import dlt
from pyspark.sql.functions import *

#gold streaming view on top of silver streaming view (not on silver table)

@dlt.view(
    name='sales_gold_view'
)

def sales_gold_view():
    df= spark.readStream.table('sales_silver_view')
    return df


#creating table with auto CDC



dlt.create_streaming_table(name ="fact_Sales")


dlt.create_auto_cdc_flow(
    source = "sales_gold_view",
    target = "fact_Sales",
    keys= ['sales_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=1

)
