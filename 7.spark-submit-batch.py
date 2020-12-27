# Run Spark Submit with parameters, e.g.
# /spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] my_script.py
# my_script.py is the name of this Python file

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

spark = SparkSession.builder.appName("my_spark_2.4").getOrCreate()

# schema for data from file e-commerce.csv
# the file is uploaded to HDFS folder: /user/BD_228_ddoni/input_csv_for_stream/
ecommerce_schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", StringType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", StringType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# read all CSV from HDFS with .read
raw_files = spark \
    .read \
    .format("csv") \
    .schema(ecommerce_schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()

# set load_time as current timestamp
# will use it in print messages and output file names
load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

print(">> Batch loading started at " + load_time)

# write output into parquet files as partitions
raw_files.withColumn("p_date", F.lit("load_time")) \
    .write \
    .mode("append") \
    .parquet("ecommerce_submit_parquet_files/p_date=" + str(load_time))

print(">> Batch loading finished at " + load_time)

# not really needed here
spark.stop()
