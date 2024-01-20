# a23_acumularTootsLang.py
# python3 a23_acumularTootsLang.py
# This code accumulates counts continuously

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Initialize SparkContext and StreamingContext with a 1-second batch interval
app_name = "AccumulateTootsLang"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[2]", appName="app_name")
except ValueError:
    sc.stop()
    sc = SparkContext("local[2]", appName="app_name")

sc.setLogLevel("ERROR")

batch_interval = 5  # Batch interval in seconds
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("checkpoint")  # Necessary for updateStateByKey operation

# Define Kafka parameters
kafka_server = 'Cloudera02:9092,Cloudera03:9092'  # Kafka server address
kafka_topic = 'mastodon_toots'   # Kafka topic
kafka_group = 'xmaltast'   # Kafka consumer group, first surname of each member of the group separated by an underscore.

kafkaParams = {
    "metadata.broker.list": kafka_server,
    "group.id": kafka_group
} 

# Create a DStream that connects to Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafkaParams)

# Update the cumulative count using updateStateByKey
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues) + runningCount

# Count each toot as 1 and update the total count
tootCounts = kafkaStream\
    .map(lambda x: json.loads(x[1]))\
    .filter(lambda toot: "language" in toot and toot["language"] is not None)\
    .map(lambda toot: (toot["language"], 1))\
    .updateStateByKey(updateFunction)\
    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Print the cumulative count
tootCounts.pprint()

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()