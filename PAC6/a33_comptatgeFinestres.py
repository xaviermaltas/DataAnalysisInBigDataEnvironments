# a33_comptatgeFinestres.py
# python3 a33_comptatgeFinestres.py

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import json_tuple, from_json, col, window, count

conf = SparkConf()
conf.setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Initialize Spark Session for Structured Streaming
app_name = "activity3_3_xmaltast"  # Replace with your Spark app name, must include the username of the members of the group

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()

# Define Kafka parameters
kafka_topic = 'mastodon_toots'
kafka_bootstrap_servers = 'Cloudera02:9092,Cloudera03:9092'  # Replace with your Kafka bootstrap servers

# Read a small batch of data from Kafka for schema inference!
batch_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Infer schema
schema = spark.read.json(batch_df.selectExpr("CAST(value AS STRING)").rdd.map(lambda x: x[0])).schema
# Create streaming DataFrame by reading data from Kafka
toots = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the value column as JSON and apply the inferred schema. Then select the columns we need.
toots_df = toots\
    .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

# Print the schema to understand the structure
toots_df.printSchema()

# Filter based on the correct nested field
filtered_toots_df = toots_df \
    .filter(col("parsed_value.quote.account.display_name") == "some_value") \
    .select(
        "parsed_value.text",            # Replace with the actual text field
        "parsed_value.created_at",      # Replace with the actual created_at field
        "parsed_value.group_by_column"  # Replace with the actual group by column
    ) \
    .groupBy(
        window("parsed_value.created_at", "5 minutes"),
        "parsed_value.group_by_column"
    ) \
    .count() \
    .orderBy("window.start", "window.end", ascending=False)

try:
    # Open stream to console (you need to execute it in a terminal to see the output)
    query = filtered_toots_df \
            .writeStream \
            .outputMode("complete")\
            .format("console")\
            .option("truncate", "false")\
            .trigger(processingTime='5 seconds')\
            .start()

    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
    sc.stop()