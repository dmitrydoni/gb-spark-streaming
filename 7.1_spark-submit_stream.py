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

# read all CSV from HDFS with .readStream
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(ecommerce_schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()

# set load_time as current timestamp
load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# the sink will write all stream data to the same directory
def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path", "ecommerce_submit_parquet_files/p_date=" + str(load_time)) \
        .option("checkpointLocation", "ecommerce_parquet_checkpoint") \
        .start()

# add load time to the files
timed_files = raw_files.withColumn("p_date", F.lit("load_time"))

# run stream to always white into the same directory
stream = file_sink(timed_files, 10)

# spark.stop() not needed
# Stream will finish right away
