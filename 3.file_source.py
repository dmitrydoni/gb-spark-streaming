from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, DateType


spark = SparkSession.builder.appName("my_spark_2.3").getOrCreate()


# function for output to console instead of show()
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()


# FILE SOURCE
# schema must be specified when creating a streaming source DataFrame
ecommerce_schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", StringType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", StringType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# run with schema 'ecommerce_schema' defined for data
# input data is in CSV file in HDFS directory: /user/BD_228_ddoni/input_csv_for_stream/
raw_files = spark \
    .readStream \
    .format("csv") \
    .option("sep", ",") \
    .schema(ecommerce_schema) \
    .options(path="input_csv_for_stream",
             header=True,
             maxFilesPerTrigger=1) \
    .load()

out = console_output(raw_files, 5)
out.stop()

# add other columns
extra_files = raw_files \
    .withColumn("DescriptionLength", F.length(F.col("Description")))\
    .withColumn("InvoiceDayOfWeek", F.dayofweek(F.col("InvoiceDate")))\
    .filter(F.col("DescriptionLength") > 10)

out = console_output(extra_files, 5)
out.stop()
