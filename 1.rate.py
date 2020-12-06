from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

# this string is not needed in pyspark console:
# SparkSession available as 'spark'
spark = SparkSession.builder.appName("my_spark_2.3").getOrCreate()

# RATE SOURCE
raw_rate = spark \
    .readStream \
    .format("rate") \
    .load()

raw_rate.printSchema()

raw_rate.isStreaming

# will not work becase raw_rate is a streaming df
raw_rate.show()

stream = raw_rate.writeStream \
    .format("console") \
    .start()   # start streaming messages every 1 sec

stream.stop()

# run streaming every 30 sec
stream = raw_rate \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .format("console") \
    .options(truncate=False) \
    .start()

# check parameters
stream.explain()
stream.isActive
stream.lastProgress
stream.status

stream.stop()


# output to console instead of show()
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()

out = console_output(raw_rate, 5)
out.stop()

# add a custom filter
filtered_rate = raw_rate \
    .filter( F.col("value") % F.lit("2") == 0 )

out = console_output(filtered_rate, 5)
out.stop()

# add custom columns
extra_rate = filtered_rate \
    .withColumn("my_value",
                F.when((F.col("value") % F.lit(10) == 0), F.lit("jubilee"))
                    .otherwise(F.lit("not yet")))

out = console_output(extra_rate, 5)
out.stop()

# stop all active streams (get from Spark environment)
# may need in case we lose the stream from a variable
def killAll():
    for active_stream in spark.streams.active:
        print("Stopping %s by killAll" % active_stream)
        active_stream.stop()
