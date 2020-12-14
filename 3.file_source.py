from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType


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
demography_schema = StructType() \
    .add("year", IntegerType(), nullable=True) \
    .add("region", StringType(), nullable=True) \
    .add("npg", FloatType(), nullable=True) \
    .add("birth_rate", FloatType(), nullable=True) \
    .add("death_rate", FloatType(), nullable=True) \
    .add("migratory_growth", FloatType(), nullable=True) \
    .add("population", IntegerType(), nullable=True)

happiness_schema = StructType() \
    .add("country", IntegerType()) \
    .add("ladder", IntegerType()) \
    .add("sd_of_ladder", IntegerType()) \
    .add("positive_affect", IntegerType()) \
    .add("social_support", FloatType()) \
    .add("freedom", FloatType()) \
    .add("corruption", IntegerType()) \
    .add("generosity", IntegerType()) \
    .add("gdp_per_capita", IntegerType()) \
    .add("life_expectancy", IntegerType())

personality_schema = StructType() \
    .add("userid", StringType()) \
    .add("openness", FloatType()) \
    .add("agreeableness", FloatType()) \
    .add("emotional_stability", FloatType()) \
    .add("conscientiousness", FloatType()) \
    .add("extraversion", FloatType()) \
    .add("assigned metric", StringType()) \
    .add("assigned condition", StringType())

# run with schema 'demography_schema' defined for data
# input data is in CSV file in HDFS directory: /user/BD_228_ddoni/input_csv_for_stream/
raw_files = spark \
    .readStream \
    .format("csv") \
    .option("sep", ",") \
    .schema(demography_schema) \
    .options(path="input_csv_for_stream",
             header=True,
             maxFilesPerTrigger=1) \
    .load()

out = console_output(raw_files, 5)
out.stop()

# add other columns
extra_files = raw_files \
    .withColumn("country_length", F.length(F.col("country")))\
    .filter(F.col("country_length") > 10)

out = console_output(extra_files, 5)
out.stop()
