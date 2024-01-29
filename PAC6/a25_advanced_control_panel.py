# a25_advanced_control_panel.py
# python3 a25_advanced_control_panel.py

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.serializers import PickleSerializer
import json

# Initialize SparkContext
app_name = "AdvancedControlPanel"  # Name of your application

try:
    sc = SparkContext("local[2]", appName=app_name, serializer=PickleSerializer())
except ValueError:
    sc.stop()
    sc = SparkContext("local[2]", appName=app_name, serializer=PickleSerializer())

sc.setLogLevel("ERROR")

# Initialize StreamingContext with a 5-second batch interval
batch_interval = 5
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

def updateFunction(newValues, runningState):
    if runningState is None:
        runningState = {
            'total_toots': 0,
            'total_length': 0,
            'most_followed_user': ('', 0),
            'user_followers': {}
        }

    num_new_toots = sum(newValues)
    total_toots = runningState['total_toots'] + num_new_toots

    new_toots = kafkaStream\
        .map(lambda x: json.loads(x[1]))\
        .filter(lambda toot: "language" in toot and toot["language"] is not None and "content" in toot)\
        .take(num_new_toots)

    total_length = runningState['total_length'] + sum(len(toot["content"]) for toot in new_toots)

    most_followed_user = max(new_toots, key=lambda toot: toot.get("account", {}).get("followers_count", 0))
    current_followers = most_followed_user.get("account", {}).get("followers_count", 0)

    if current_followers > runningState['most_followed_user'][1]:
        most_followed_user_tuple = (most_followed_user["account"]["acct"], current_followers)
    else:
        most_followed_user_tuple = runningState['most_followed_user']

    # Track followers count for each user
    user_followers = runningState['user_followers']
    for toot in new_toots:
        user_acct = toot["account"]["acct"]
        followers_count = toot.get("account", {}).get("followers_count", 0)
        user_followers[user_acct] = followers_count

    updated_state = {
        'total_toots': total_toots,
        'total_length': total_length,
        'most_followed_user': most_followed_user_tuple,
        'user_followers': user_followers
    }

    return updated_state

# Create a DStream that connects to Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafkaParams)

# Count each toot as 1 and update the total count
tootCounts = kafkaStream\
    .map(lambda x: json.loads(x[1]))\
    .filter(lambda toot: "language" in toot and toot["language"] is not None)\
    .map(lambda toot: (toot["language"], 1))\
    .updateStateByKey(updateFunction)\
    .window(windowDuration=window_duration, slideDuration=slide_duration)\
    .reduceByKey(lambda x, y: x + y)\
    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Calculate average length, most followed user, and format the output
def processAverages(_, rdd):
    rdd_data = rdd.collect()
    if rdd_data:
        rdd_data = rdd_data[:10]
        result = [(lang, {
            'numtotoots': count,
            'avglen_delcontent': data['total_length'] / data['total_toots'] if data['total_toots'] > 0 else 0,
            'most_followed_user': data['most_followed_user'][0],
            'followers': data['most_followed_user'][1]
        }) for (lang, count), data in rdd_data]
        print(result)

# Print the advanced control panel
tootCounts.foreachRDD(processAverages)

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()
