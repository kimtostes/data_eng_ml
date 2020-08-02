#Author: Kim Tostes
#Version 1.0
#trusted_orders.py
#This code needs ifood.raw_orders, ifood.raw_consumer, ifood.raw_restaurant and ifood.raw_status tables

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
        
logging.info("Reading definitions...")
#File location, type, databases/tables used, way of partitioning
database_name = "ifood"
table_name = "trusted_orders"
partitions = ["delivery_address_country", "delivery_address_state", "delivery_address_city"]
path_to_save = f"/user/hive/warehouse/{database_name}.db/{table_name}"
table_mode = "append"
file_format_to_save = "parquet"
        
#Loading tables used for   
logging.info("Loading table ifood.orders...")
order = spark.table("ifood.orders")
logging.info("Loading table ifood.consumer...")
consumer = spark.table("ifood.consumer")
logging.info("Loading table ifood.restaurant...")
restaurant = spark.table("ifood.restaurant")
logging.info("Loading table ifood.status...")
status = spark.table("ifood.status")

#Join order to consumer
logging.info("Joining consumer x order ...")
consumer = consumer.withColumnRenamed("created_at", "consumer_created_at")
consumer_join_order = consumer.join(order, on=["customer_id", "customer_name"], how='inner')

#More joins 
logging.info("Joining consumer/order x restaurant ...")
restaurant = restaurant.withColumnRenamed("created_at", "restaur_created_at")
consumer_join_order_join_restaurant = consumer_join_order.join(restaurant, consumer_join_order.merchant_id == restaurant.id , how='left')

#Preparing last order table 
tmp = Window.partitionBy('order_id')
last_status = status.withColumn('max_tmp', F.max('created_at').over(tmp))\
    .where(F.col('created_at') == F.col('max_tmp'))\
    .drop('max_tmp')
    
last_status = last_status.select("order_id", "value")

#Final join - adding last status
df = consumer_join_order_join_restaurant.join(last_status, on=["order_id"], how='left')

#dropDuplicates 
logging.info("Dropping duplicates...")
df.dropDuplicates()

#newlist = df.columns
#newlist = newlist.remove("order_id")
#df = df.groupBy(F.col("order_id")).agg(list(map(lambda arg: "F.collect_set("+"\""+arg+"\""+")", newlist)))


#save to table
#partion = 
logging.info("Saving to table...")
df.write.partitionBy(partitions).saveAsTable(database_name+"."+table_name, format=file_format_to_save, mode=table_mode, path=path_to_save)  