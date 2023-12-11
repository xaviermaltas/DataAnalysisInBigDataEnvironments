#Topic creation
kafka-topics --create --zookeeper localhost:2181/kafka --topic PEC5xmaltast --partitions 1 --replication-factor 1 --config retention.ms=7200000

#List topics
kafka-topics --zookeeper localhost:2181/kafka --list

#Describe topic
kafka-topics --zookeeper localhost:2181/kafka --describe --topic PEC5xmaltast

#Delete topic
kafka-topics --zookeeper localhost:2181/kafka --delete --topic PEC5xmaltast

#kafka console producer
kafka-console-producer --broker-list Cloudera02:9092,Cloudera03:9092 --topic PEC5xmaltast

#kafka console consumer
kafka-console-consumer --bootstrap-server Cloudera02:9092,Cloudera03:9092 --topic PEC5xmaltast