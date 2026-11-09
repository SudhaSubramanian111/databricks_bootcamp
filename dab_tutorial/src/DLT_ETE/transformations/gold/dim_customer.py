import dlt
from pyspark.sql.functions import *

#gold streaming view on top of silver streaming view (not on silver table)

@dlt.view(
    name='customer_gold_view'
)

def stores_gold_view():
    df= spark.readStream.table('customers_silver_view')
    return df


#creating table with auto CDC



dlt.create_streaming_table(name ="dim_customers")


dlt.create_auto_cdc_flow(
    source = "customer_gold_view",
    target = "dim_customers",
    keys= ['customer_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']

)
