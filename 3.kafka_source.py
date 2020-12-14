# Before starting Spark, run:
# export SPARK_KAFKA_VERSION=0.10

# Start Spark with v2.11 packages:
# pyspark2 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("my_spark_2.3").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

# function for output to console instead of show()
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()


# read without streaming
raw_orders = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    load()

raw_orders.show()
raw_orders.show(1,False)

raw_locations = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "dd_locations"). \
    option("startingOffsets", "earliest"). \
    load()

raw_locations.show()

# read offsets 0-10
raw_locations = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "dd_locations"). \
    option("startingOffsets", "earliest"). \
    option("endingOffsets", """{"orders_json":{"0":10}}"""). \
    load()

raw_locations.show(100)

# read all
raw_locations = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "dd_locations"). \
    option("startingOffsets", "earliest"). \
    load()

out = console_output(raw_locations, 5)
out.stop()

# read in batches of 5
raw_locations = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "dd_locations"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_locations, 5)
out.stop()


# read once from end
raw_locations = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "dd_locations"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "latest"). \
    load()

out = console_output(raw_locations, 5)
out.stop()


# read offsets 0-10
raw_locations = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "dd_locations"). \
    option("startingOffsets", """{"orders_json":{"0":10}}"""). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_locations, 5)
out.stop()

# parse value
# define schema
locations_schema = StructType() \
    .add("timestamp", IntegerType()) \
    .add("latitude", IntegerType()) \
    .add("longitude", IntegerType()) \
    .add("accuracy", IntegerType())

value_locations = raw_locations \
    .select(F.from_json(F.col("value").cast("String"), locations_schema).alias("value"), "offset")

value_locations.printSchema()

parsed_locations = value_locations.select("value.*", "offset")

parsed_locations.printSchema()

out = console_output(parsed_locations, 30)
out.stop()

# add checkpoint
def console_output_checkpointed(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate",False) \
        .option("checkpointLocation", "locations_console_checkpoint") \
        .start()

out = console_output_checkpointed(parsed_locations, 5)
out.stop()
