# a21_comptarFinestres.py
# python3 a21_comptarFinestres.py

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Initialize SparkContext and StreamingContext with a 1-second batch interval
app_name = "TootCount"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[2]", appName = app_name)
except ValueError:
    sc.stop()
    sc = SparkContext("local[2]", appName = app_name)
sc.setLogLevel("ERROR")

# Create the StreamingContext
batch_interval = 5  # Batch interval in seconds
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("checkpoint")  # Necessary for updateStateByKey operation

# Define Kafka parameters
kafka_server = 'Cloudera02:9092,Cloudera03:9092'  # Kafka server address
kafka_topic = "mastodon_toots"   # Kafka topic
kafka_group = "xmaltast"   # Kafka consumer group, first surname of each member of the group separated by an underscore.

kafkaParams = {
    "metadata.broker.list": kafka_server,
    "group.id": kafka_group
} 

# Create a DStream that connects to Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafkaParams)

# Count each toot as 1 and update the total count excluding retweets
tootCounts = kafkaStream\
    .map(lambda x: json.loads(x[1]))\
    .filter(lambda toot: 'retweeted_status' not in toot) \
    .map(lambda x: ("toot", 1))\
    .updateStateByKey(lambda values, total: sum(values) + (total or 0))

# Print the cumulative count every 5 seconds
tootCounts.pprint()

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()