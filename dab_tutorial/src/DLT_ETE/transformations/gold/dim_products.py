import dlt
from pyspark.sql.functions import *

#gold streaming view on top of silver streaming view (not on silver table)

@dlt.view(
    name='products_gold_view'
)

def stores_gold_view():
    df= spark.readStream.table('products_silver_view')
    return df


#creating table with auto CDC



dlt.create_streaming_table(name ="dim_products")


dlt.create_auto_cdc_flow(
    source = "products_gold_view",
    target = "dim_products",
    keys= ['product_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']

)
