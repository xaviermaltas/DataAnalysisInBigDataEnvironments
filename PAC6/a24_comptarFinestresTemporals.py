# a24_comptarFinestresTemporals.py
# python3 a24_comptarFinestresTemporals.py

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Initialize SparkContext and StreamingContext with a 1-second batch interval
app_name = "FILLIN"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[2]", appName="app_name")
except ValueError:
    sc.stop()
    sc = SparkContext("local[2]", appName="app_name")

sc.setLogLevel("ERROR")

ssc = StreamingContext(<FILLIN>)
ssc.checkpoint("checkpoint")  # Necessary for updateStateByKey operation

# Define Kafka parameters
kafka_server = <FILLIN>  # Kafka server address
kafka_topic = <FILLIN>   # Kafka topic
kafka_group = <FILLIN>         # Kafka consumer group, first surname of each member of the group separated by an underscore.

kafkaParams = {
    "metadata.broker.list": kafka_server,
    "group.id": kafka_group
} 


# Create a DStream that connects to Kafka
kafkaStream = KafkaUtils.createDirectStream(<FILLIN> )

# Count each toot as 1 and update the total count. Use a 60-second window with a 5-second slide
tootCounts = kafkaStream\
    .<FILLIN>

# Print the cumulative count
tootCounts.pprint()

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()