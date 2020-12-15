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

# create topic "yelp_tips" using kafka-topics.sh
# send serialized JSON to the topic using kafka-console-producer.sh
# here, in Spark, read the topic without streaming
raw_tips = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", "earliest"). \
    load()

raw_tips.show()
raw_tips.show(1, False)

# read offsets 0-10
raw_tips = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", "earliest"). \
    option("endingOffsets", """{"yelp_tips":{"0":10}}"""). \
    load()

raw_tips.show(100)

# read all to stream DF
raw_tips = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", "earliest"). \
    load()

out = console_output(raw_tips, 5)
out.stop()

# read in batches of 5
raw_tips = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_tips, 5)
out.stop()


# read once from end
raw_tips = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "latest"). \
    load()

out = console_output(raw_tips, 5)
out.stop()


# read offsets 0-10 in batches of 5
raw_tips = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", """{"yelp_tips":{"0":10}}"""). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_tips, 5)
out.stop()

# parse value
# define schema
tips_schema = StructType() \
    .add("user_id", StringType()) \
    .add("business_id", StringType()) \
    .add("text", StringType()) \
    .add("date", StringType()) \
    .add("compliment_count", IntegerType())

# read the yelp_tips columns: "value" as json, "offset" as is
value_tips = raw_tips \
    .select(F.from_json(F.col("value").cast("String"), tips_schema) \
    .alias("value"), "offset")

value_tips.printSchema()

# in this dataframe, the columns from "value" and the "offset" column
# will be on the same level
parsed_tips = value_tips.select("value.*", "offset")

parsed_tips.printSchema()

out = console_output(parsed_tips, 30)
out.stop()

# add checkpoint
def console_output_checkpoint(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate", False) \
        .option("checkpointLocation", "tips_console_checkpoint") \
        .start()

out = console_output_checkpoint(parsed_tips, 5)
out.stop()
