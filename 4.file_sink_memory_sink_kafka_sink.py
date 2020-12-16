# Before starting Spark, run in terminal:
# export SPARK_KAFKA_VERSION=0.10

# Start Spark 2.4 with Kafka v2.11 packages:
# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[1]

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("my_spark_2.4").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

# read Kafka topic "yelp_tips" to streaming DF "raw_tips"
# in batches of 5 events
raw_tips = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

# for parsing the value, define schema
tips_schema = StructType() \
    .add("user_id", StringType()) \
    .add("business_id", StringType()) \
    .add("text", StringType()) \
    .add("date", StringType()) \
    .add("compliment_count", IntegerType())

# first, parse the yelp_tips columns: "value" as json, "offset" as is
# then arrange "value" columns and "offset" on the same level
parsed_tips = raw_tips \
    .select(F.from_json(F.col("value").cast("String"), tips_schema) \
    .alias("value"), "offset") \
    .select("value.user_id", \
            "value.text", \
            "value.date", \
            "value.compliment_count", \
            "offset")

parsed_tips.printSchema()

# function for output to console
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

out = console_output(parsed_tips, 5)
out.stop()

# MEMORY SINK
def memory_sink(df, freq):
    return df.writeStream.format("memory") \
        .queryName("tips_memory_sink_table") \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()

# start reading into memory (no output to the console)
stream = memory_sink(parsed_tips, 5)

# query the memory table to view and analyze the data
spark.sql("SELECT * FROM tips_memory_sink_table").show(10)
spark.sql("SELECT DISTINCT compliment_count FROM tips_memory_sink_table").show()
spark.sql("""
    SELECT 
        user_id,
        text,
        date, 
        DATEDIFF(current_timestamp(), date) AS diff_days,
        YEAR(current_timestamp()) - YEAR(date) AS diff_years
    FROM 
        tips_memory_sink_table
    """).show(5)

# create a static dataframe based on the table
# similar to df.CreateTempView
tips_ones_static_df = spark.table("tips_memory_sink_table") \
    .filter("compliment_count = 1")

# work with the dataframe using Spark API
# count will grow because records are being added to the table
tips_ones_static_df.count()
tips_ones_static_df.show()

stream.stop()

# the static dataframe is still there but is no longer growing
tips_ones_static_df.count()

# FILE SINK
# checkpoint is mandatory for file sink
# file path and checkpoint metadata paths are on HDFS
def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path", "tips_parquet_sink") \
        .option("checkpointLocation", "tips_parquet_checkpoint") \
        .start()

stream = file_sink(parsed_tips, 5)

# check HDFS folders "tips_parquet_sink" and "tips_parquet_checkpoint"
# raw_tips is defined with option("maxOffsetsPerTrigger", "5"),
# so each file contains 5 messages
# as a good practice, we can save files
# to a subdirectory (e.g. "tips_parquet_sink/p_date_20201216")
# and change directory name daily

stream.stop()

# COMPACTION
# small files are not good for HDFS, so we should collect them
# into large files regularly (e.g. on the next day)
# this compaction can be improved as follows:
# first save to a temp directory
# then create one file from temp directory
# then delete temp dir
def compact_directory(path):
    df_to_compact = spark.read.parquet(path + "/*.parquet")
    df_to_compact.persist() # write data into RAM
    df_to_compact.count()  # this activates persist()
    df_to_compact.repartition(1).write.mode("overwrite").parquet(path)
    df_to_compact.unpersist()

compact_directory("tips_parquet_sink")

# add a column with random numbers 0..100
parsed_tips_ext = parsed_tips \
    .withColumn("random_number", F.round(F.rand() * 100))

# KAFKA SINK
# the df written to Kafka should have a specific set of columns:
# key (optional), value (required), topic (optional)
# only String or Binary types - that is why CAST AS STRING
# the "value" column will be a comma-separated list of values (CSV)
def kafka_sink(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", \
                         "CAST(struct(*) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "tips_kafka_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "tips_kafka_checkpoint") \
        .start()

stream = kafka_sink(parsed_tips_ext, 5)
stream.stop()

# the "value" column will be a JSON
def kafka_sink_json(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", \
                         "CAST(to_json(struct(*)) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "tips_kafka_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "tips_kafka_checkpoint") \
        .start()

stream = kafka_sink_json(parsed_tips_ext, 5)
stream.stop()
