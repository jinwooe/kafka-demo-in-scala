# kafka-dmeo-in-scala

	# bin/zookeeper-server-start.sh config/zookeeper.properties
	# bin/kafka-server-start.sh config/server.properties
	# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 3 --topic amazingTopic

	// topic, number of message
	# sbt "run-main SimpleProducer amazingTopic 100"
	
	// zookeeper, consumer group id, topic
	# sbt "run-main SimpleConsumer localhost:2181 testGroup amazingTopic"
	
	// zookeeper, consumer group id, topicm thread count to perform
	# sbt "run-main MultiThreadConsumer localhost:2181 testGroup amazingTopic 5"
