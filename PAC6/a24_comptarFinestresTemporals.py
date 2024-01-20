# a24_comptarFinestresTemporals.py
# python3 a24_comptarFinestresTemporals.py
# This code provides counts within time-based windows, offering more insights into the distribution of toots over specified intervals. 

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Initialize SparkContext and StreamingContext with a 1-second batch interval
app_name = "TopLanguagesWindowedCounts"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[2]", appName="app_name")
except ValueError:
    sc.stop()
    sc = SparkContext("local[2]", appName="app_name")

sc.setLogLevel("ERROR")

batch_interval = 5  # Batch interval in seconds
window_duration = 60  # Window duration in seconds
slide_duration = 5  # Slide duration in seconds

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

# Count each toot as 1 and update the total count. Use a 60-second window with a 5-second slide
tootCounts = kafkaStream\
    .map(lambda x: json.loads(x[1]))\
    .filter(lambda toot: "language" in toot and toot["language"] is not None)\
    .map(lambda toot: (toot["language"], 1))\
    .updateStateByKey(lambda new_values, running_count: sum(new_values) + (running_count or 0))\
    .window(windowDuration=window_duration, slideDuration=slide_duration)\
    .reduceByKey(lambda x, y: x + y)\
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