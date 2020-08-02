#Author: Kim Tostes
#Version 1.0
#trusted_items.py
#This code needs ifood.raw_orders table

from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
import logging

#Init spark session
spark = SparkSession.builder\
        .appName("trusted_order") \
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse')\
        .enableHiveSupport()\
        .getOrCreate()
        
#File location, type, databases/tables used, way of partitioning
logging.info("Reading definitions...")
database_name = "ifood"
table_name = "trusted_items"
partitions = ["value"]
path_to_save = f"/user/hive/warehouse/{database_name}.db/{table_name}"
table_mode = "append"
file_format_to_save = "parquet"
        
#Loading tables used for  
logging.info("Loading table ifood.orders...")
df = spark.table("ifood.orders")

#Select data needed
logging.info("Getting data...")
df = df.select("order_id", "items")

#save tmp json loaded from column items from orders
df.write.format("text").option("header", False).save("/tmp/items.json")

#read items.json 
df = spark.read.json("/tmp/items.json")

#save to table
logging.info("Saving to table...")
df.write.partitionBy(partitions).saveAsTable(database_name+"."+table_name, format=file_format_to_save, mode=table_mode, path=path_to_save)  