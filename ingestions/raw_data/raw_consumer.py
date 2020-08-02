#Author: Kim Tostes
#Version 1.0
#raw_consumer.py

from pyspark.sql import SparkSession
import logging

#Init spark session
logging.info("Initializing Spark Session...")
spark = SparkSession.builder\
        .appName("raw_consumer") \
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse')\
        .enableHiveSupport()\
        .getOrCreate()

logging.info("Reading definitions...")
#File location, type, databases/tables used, way of partitioning
#file_location = "s3://ifood-data-architect-test-source/consumer.csv.gz"
file_location = "/input/ifood/consumer.csv.gz"
file_type = "csv"
database_name = "ifood"
table_name = "consumer"
partitions = ["customer_phone_area"]
path_to_save = f"/user/hive/warehouse/{database_name}.db/{table_name}"
table_mode = "append"
file_format_to_save = "parquet"

#CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

#Read CSV
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#dropDuplicates 
logging.info("Dropping duplicates...")
df.dropDuplicates()

#save to table
#partion = 
logging.info("Saving to table...")
df.write.partitionBy(partitions).saveAsTable(database_name+"."+table_name, format=file_format_to_save, mode=table_mode, path=path_to_save)



