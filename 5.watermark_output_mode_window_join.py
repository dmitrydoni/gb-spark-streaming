#export SPARK_KAFKA_VERSION=0.10
# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --master local[1]
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("my_spark_2.4").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

# read Kafka topic
raw_tips = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

# define schema
tips_schema = StructType() \
    .add("user_id", StringType()) \
    .add("business_id", StringType()) \
    .add("text", StringType()) \
    .add("date", StringType()) \
    .add("compliment_count", IntegerType())

# parse "value" and "offset"
parsed_tips = raw_tips \
    .select(F.from_json(F.col("value").cast("String"), tips_schema) \
    .alias("value"), "offset") \
    .select("value.user_id", \
            "value.text", \
            "value.date", \
            "value.compliment_count", \
            "offset")

# add a custom column
extended_tips = parsed_tips \
    .withColumn("tip_receive_time", F.current_timestamp())

extended_tips.printSchema()

# checkpoint is needed for moving cache from memory to HDFS
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("checkpointLocation", "checkpoints/duplicates_console_chk") \
        .options(truncate=False) \
        .start()

stream = console_output(extended_tips, 2)
stream.stop()

# WATERMARK should reset checkpoint
# it may do it later but not earlier than specified (1 day here)
waterwarked_tips = extended_tips.withWatermark("tip_receive_time", "1 day")
waterwarked_tips.printSchema()

# drop duplicates for each tick (batch)
deduplicated_tips = waterwarked_tips.drop_duplicates(["user_id", "tip_receive_time"])

stream = console_output(deduplicated_tips, 20)
stream.stop()


# WINDOW - deduplicate for a period of time ("window")
# create a time window
windowed_tips = extended_tips \
        .withColumn("window_time", \
                    F.window(F.col("tip_receive_time"), "2 minute"))
windowed_tips.printSchema()

stream = console_output(windowed_tips, 20)
stream.stop()

# set watermark for the cleanup of checkpoint
waterwarked_windowed_tips = windowed_tips \
                        .withWatermark("window_time", "1 minute")
# drop duplicates in each window
deduplicated_windowed_tips = waterwarked_windowed_tips \
                    .drop_duplicates(["user_id", "window_time"])

stream = console_output(deduplicated_windowed_tips, 10)
stream.stop()


# SLIDING WINDOW
# create a window
sliding_tips = extended_tips \
            .withColumn("sliding_time", \
                        F.window(F.col("order_receive_time"), \
                                 "1 minute", "30 seconds"))
sliding_tips.printSchema()

stream = console_output(sliding_tips, 20)
stream.stop()

# set watermark for the cleanup of checkpoint
waterwarked_sliding_tips = sliding_tips \
                    .withWatermark("sliding_time", "2 minute")
# drop duplicates in each window
deduplicated_sliding_tips = waterwarked_sliding_tips \
                    .drop_duplicates(["user_id", "sliding_time"])

stream = console_output(deduplicated_sliding_tips, 20)
stream.stop()


# OUTPUT MODES
def console_output(df, freq, out_mode):
    return df.writeStream.format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .option("checkpointLocation", "checkpoints/my_watermark_console_chk2") \
        .outputMode(out_mode) \
        .start()

count_tips = waterwarked_windowed_tips \
                .groupBy("window_time").count()
# write only updated records
stream = console_output(count_tips, 20, "update")
stream.stop()

# write all records
stream = console_output(count_tips, 20, "complete")
stream.stop()

# write all records only once
stream = console_output(count_tips, 20, "append") # once at the end of watermark
stream.stop()

sliding_tips = waterwarked_sliding_tips \
                .groupBy("sliding_time").count()
# sums in sliding window
stream = console_output(sliding_tips, 20, "update")
stream.stop()

# JOINS

# stream-hdfs join
static_df = spark.table("spark_streaming_course.order_items")
static_joined = waterwarked_orders.join(static_df, "order_id", "inner" )
static_joined.isStreaming
static_joined.printSchema  # only one order_id column

selected_static_joined = static_joined.select("order_id", "order_status", "order_purchase_timestamp", "order_receive_time", "order_item_id", "product_id")

stream = console_output(selected_static_joined, 1, "update")
stream.stop()

# stream-stream join
# stream #2
raw_orders_items = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "order_items"). \
    option("startingOffsets", "earliest"). \
    load()

# parse value
schema_items = StructType() \
    .add("order_id", StringType()) \
    .add("order_item_id", StringType()) \
    .add("product_id", StringType()) \
    .add("seller_id", StringType()) \
    .add("shipping_limit_date", StringType()) \
    .add("price", StringType()) \
    .add("freight_value", StringType())

extended_orders_items = raw_orders_items \
    .select(F.from_json(F.col("value").cast("String"), schema_items).alias("value")) \
    .select("value.*") \
    .withColumn("order_items_receive_time", F.current_timestamp()) \
    .withColumn("window_time",F.window(F.col("order_items_receive_time"),"10 minute"))

extended_orders_items.printSchema()


windowed_orders = extended_orders.withColumn("window_time",F.window(F.col("order_receive_time"),"10 minute"))
waterwarked_windowed_orders = windowed_orders.withWatermark("window_time", "10 minute")


streams_joined = waterwarked_windowed_orders \
    .join(extended_orders_items, ["order_id", "window_time"] , "inner") \
    .select("order_id", "order_item_id", "product_id", "window_time")

stream = console_output(streams_joined , 20, "update") # won't work for inner
stream = console_output(streams_joined , 20, "append")
stream.stop()

streams_joined = waterwarked_windowed_orders \
    .join(extended_orders_items, ["order_id", "window_time"] , "left") \
    .select("order_id", "order_item_id", "product_id", "window_time")

stream = console_output(streams_joined , 20, "update")
stream.stop()

