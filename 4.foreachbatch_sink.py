# export SPARK_KAFKA_VERSION=0.10
# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[1]
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("my_spark_2.4").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

# read Kafka topic "yelp_tips" to streaming DF "raw_tips"
# in batches of 20 events
raw_tips = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "yelp_tips"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "20"). \
    load()

# for parsing the value, define schema
tips_schema = StructType() \
    .add("user_id", StringType()) \
    .add("business_id", StringType()) \
    .add("text", StringType()) \
    .add("date", StringType()) \
    .add("compliment_count", IntegerType())

# parse "value" and "offset", arrange columns on the same level
parsed_tips = raw_tips \
    .select(F.from_json(F.col("value").cast("String"), tips_schema) \
    .alias("value"), "offset") \
    .select("value.user_id", \
            "value.text", \
            "value.date", \
            "value.compliment_count", \
            "offset")

# add two custom columns
extended_tips = parsed_tips \
    .withColumn("score", F.round(F.rand() * 100)) \
    .withColumn("batch_processed_at", F.current_timestamp())

extended_tips.printSchema()

# FOREACH BATCH SINK
# each batch will be processed with a custom function -
# in this case, with foreach_batch_function()
def foreach_batch_sink(df, freq):
    return df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq) \
        .start()

# this function will be called for each batch
# it gets a batch as input
# and we can work with a batch like with a regular dataframe
# using Spark dataframe API
def foreach_batch_function(df, epoch_id):
    print("Starting epoch # " + str(epoch_id) + "...")
    # persist for fast in-memory processing
    # also, for multiple filtering
    df.persist()
    # create 3 different dataframes based on custom logic
    # DF for tips with "low" score
    df.filter(F.col("score") <= 50). \
        select("user_id", "text", "compliment_count", "score"). \
        withColumn("score_category", F.lit("low")). \
        show(truncate=True)
    # DF for tips with "average" score
    df.filter( (F.col("score") > 50) & (F.col("score") <= 75) ). \
        select("user_id", "text", "compliment_count", "score"). \
        withColumn("score_category", F.lit("average")). \
        show(truncate=True)
    # DF for tips with "high" score
    df.filter(F.col("score") > 75). \
        select("user_id", "text", "compliment_count", "score"). \
        withColumn("score_category", F.lit("high")). \
        show(truncate=True)
    # unpersist to clear the batch data from memory
    df.unpersist()
    print("Finishing epoch # " + str(epoch_id))


stream = foreach_batch_sink(extended_tips, 10)
stream.stop()

# each batch will be processed with a custom function -
# in this case, with foreach_batch_function_extended()
def foreach_batch_sink_extended(df, freq):
    return df \
        .writeStream \
        .foreachBatch(foreach_batch_function_extended) \
        .trigger(processingTime='%s seconds' % freq) \
        .start()

# create different dataframes based on recency
# and save the dataframes to parquet files
def foreach_batch_function_extended(df, epoch_id):
    print("Started processing batch # " + str(epoch_id))
    df.persist()
    # create a DF with the "days_passed" column
    df_days_passed = df.withColumn("days_passed", \
                  F.datediff(F.to_date("batch_processed_at"), \
                            F.to_date("date")))
    df_days_passed.persist()
    df.unpersist()
    # DF with recent tips
    df_recent_tips = df_days_passed.filter(F.col("days_passed") <= 2000). \
        select("offset", "text", "score", "date"). \
        withColumn("recency", F.lit("recent"))
    # save DF with recent tips data for multiple actions
    df_recent_tips.persist()
    print("Saving recent tips data:")
    df_recent_tips.show(10, truncate=True)
    # save recent tips to HDFS
    df_recent_tips.write.mode("append").parquet("recent_tips")
    print("Recent tips written: " + str(df_recent_tips.count()))
    # remove DF with recent tips data from memory
    df_recent_tips.unpersist()
    df_old_tips = df_days_passed.filter(F.col("days_passed") > 2000). \
         select("offset", "text", "score", "date"). \
         withColumn("recency", F.lit("old"))
    # save DF with old tips data for multiple actions
    df_old_tips.persist()
    print("Saving old tips data:")
    df_old_tips.show(10, truncate=True)
    # save old tips to HDFS
    df_old_tips.write.mode("append").parquet("old_tips")
    print("Old tips written: " + str(df_old_tips.count()))
    # remove DF with old tips data from memory
    df_old_tips.unpersist()
    # remove all batch data from memory
    df_days_passed.unpersist()
    print("Finished processing batch # " + str(epoch_id))


stream = foreach_batch_sink_extended(extended_tips, 30)
stream.stop()
