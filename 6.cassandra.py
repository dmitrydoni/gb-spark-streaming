# export SPARK_KAFKA_VERSION=0.10
# Run spark with Cassandra connector
# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("my_spark_2.4_with_Cassandra").getOrCreate()

def explain(self, extended=True):
    if extended:
        print(self._jdf.queryExecution().toString())
    else:
        print(self._jdf.queryExecution().simpleString())

# DF from the "animals" table that was created in Cassandra
cass_animals_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .load()

cass_animals_df.printSchema()
cass_animals_df.show()

cass_animals_df.write.parquet("animals_parquet_from_cassandra")

# create a DF from a SQL expression
cow_df = spark.sql("""select 11 as id, "Cow" as name, "Big" as size """)
cow_df.show()

# write DF to Cassandra
# Cassandra host is in spark-defaults.conf
cow_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .mode("append") \
    .save()

# same id=11
bull_df = spark.sql("""select 11 as id, "Bull" as name, "Big" as size """)
bull_df.show()

# write DF to Cassandra - this will result in update of id=11
bull_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .mode("append") \
    .save()

# read large dataset by key
cass_big_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_many", keyspace="keyspace1") \
    .load()

cass_big_df.show()

# filter by key
cass_big_df.filter(F.col("user_id") == "10").show()

# Physical Plan
# fast query (by id):
# PushedFilters: [IsNotNull(user_id), *EqualTo(user_id,10]
# filtering is done by Cassandra
explain(cass_big_df.filter(F.col("user_id") == "10"))
# slow query (by value):
# PushedFilters: [IsNotNull(gender), *EqualTo(gender,10]
explain(cass_big_df.filter(F.col("gender") == "10"))

# BETWEEN is not pushed to Cassandra:
cass_big_df.createOrReplaceTempView("cass_df")
sql_select = spark.sql("""
select * from cass_df where user_id between 1999 and 2000
""")
# user_id is not in pushedFilter
explain(sql_select)
sql_select.show()  # slow

# IN can be pushed to Cassandra:
sql_select = spark.sql("""
select * from cass_df where user_id in (3884632855,3562535987)
""")

# pushedFilter contains user_id, so filtering is performed by Cassandra
explain(sql_select)
sql_select.show()  # fast