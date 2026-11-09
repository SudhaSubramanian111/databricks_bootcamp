import dlt

@dlt.table(
    name = 'sales_bronze'
)

def sales_bronze():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","csv")\
              .load("/Volumes/databricksudha/bronze/bronze_volume/Sales/")
            
    return df


@dlt.table(
    name = 'store_bronze'
)
def store_bronze():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","csv")\
              .load("/Volumes/databricksudha/bronze/bronze_volume/store/")
            
    return df

@dlt.table(
    name = 'products_bronze'
)

def products_bronze():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","csv")\
              .load("/Volumes/databricksudha/bronze/bronze_volume/products/")
            
    return df


@dlt.table(
    name = 'customer_bronze'
)

def customer_bronze():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","csv")\
              .load("/Volumes/databricksudha/bronze/bronze_volume/customer/")
            
    return df








