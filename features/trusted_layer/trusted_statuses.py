#Author: Kim Tostes
#Version 1.0
#trusted_status.py
#This code needs ifood.status tables

from pyspark.sql import SparkSession
import logging

#Init spark session
spark = SparkSession.builder\
        .appName("EMR") \
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse')\
        .config("spark.sql.files.ignoreCorruptFiles", 'true')\
        .enableHiveSupport()\
        .getOrCreate()
        
logging.info("Reading definitions...")
#File location, type, databases/tables used, way of partitioning
database_name = "ifood"
table_name = "trusted_statuses"
partitions = ["delivery_address_country", "delivery_address_state", "delivery_address_city"]
path_to_save = f"/user/hive/warehouse/{database_name}.db/{table_name}"
table_mode = "append"
file_format_to_save = "parquet"

#Loading tables used for   
logging.info("Loading table ifood.status...")
df = spark.table("ifood.status")
#Transformations to one line per order
df = df.groupBy(F.col("order_id")).pivot("value").agg(F.max("created_at"))

df.write.saveAsTable("ifood.order_status_tl", format='parquet', mode='overwrite', path='/user/hive/warehouse/ifood.db/order_status_tl')
#.partitionBy("concluded", "merchant_state", "merchant_city") \
