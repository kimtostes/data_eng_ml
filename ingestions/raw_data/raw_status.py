#Author: Kim Tostes
#Version 1.0
#raw_items.py

from pyspark.sql import SparkSession
import logging

#Init spark session
logging.info("Initializing Spark Session...")
spark = SparkSession.builder\
        .appName("raw_status") \
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse')\
        .enableHiveSupport()\
        .getOrCreate()

#File location, type, databases/tables used, way of partitioning
logging.info("Reading definitions...")
#file_location = "s3://ifood-data-architect-test-source/status.json.gz"
file_location = "/input/ifood/status.json.gz"
database_name = "ifood"
table_name = "status"
partitions = ["value"]
path_to_save = f"/user/hive/warehouse/{database_name}.db/{table_name}"
table_mode = "append"
file_format_to_save = "parquet"

#read file from the source
logging.info("Reading file...")
df = spark.read.json(file_location)

#dropDuplicates 
logging.info("Dropping duplicates...")
df.dropDuplicates()

#save to table
#partion = 
logging.info("Saving to table...")
df.write.partitionBy(partitions).saveAsTable(database_name+"."+table_name, format=file_format_to_save, mode=table_mode, path=path_to_save)
