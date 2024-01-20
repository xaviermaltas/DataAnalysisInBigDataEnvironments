import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Initialize SparkContext and StreamingContext with a 1-second batch interval
app_name = <FILLIN>  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[2]", appName="app_name")
except ValueError:
    sc.stop()
    sc = SparkContext("local[2]", appName="app_name")

sc.setLogLevel("ERROR")

batch_interval = 5  # Batch interval in seconds
ssc = StreamingContext(<FILLIN>)
ssc.checkpoint("checkpoint")  # Necessary for updateStateByKey operation

# Define Kafka parameters
kafka_server = <FILLIN>  # Kafka server address
kafka_topic = <FILLIN>   # Kafka topic
kafka_group = <FILLIN>   # Kafka consumer group, first surname of each member of the group separated by an underscore.

kafkaParams = {
    "metadata.broker.list": kafka_server,
    "group.id": kafka_group
} 


# Create a DStream that connects to Kafka
kafkaStream = KafkaUtils.createDirectStream(<FILLIN>)

# Update the cumulative count using updateStateByKey
def updateFunction(newValues, runningCount):
    <FILLIN>
    return <FILLIN>

# Count each toot as 1 and update the total count
tootCounts = kafkaStream\
    .map(lambda x: json.loads(x[1]))\
    .<FILLIN>
    ....
    .<FILLIN>
    .updateStateByKey(<FILLIN>)\
    <FILLIN>

# Print the cumulative count
tootCounts.pprint()

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()