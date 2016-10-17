import java.util
import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream, SimpleConsumer}

import scala.collection.JavaConverters._

/**
  * Created by jadeim on 2016. 10. 17..
  */
object SimpleConsumer {


  def main(args: Array[String]): Unit = {
    val zookeeper = args(0)
    val groupId = args(1)
    val topic = args(2)
    val simpleHLConsumer = new SimpleConsumer(zookeeper, groupId, topic)
    simpleHLConsumer.testConsumer()
  }
}

class SimpleConsumer(zookeeper: String, groupId: String, private val topic: String) {
  private val consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId))

  def testConsumer() {
    val topicMap = new util.HashMap[String, Integer]
    topicMap.put(topic, 1)
    val consumerStreamsMap = consumer.createMessageStreams(topicMap)
    val streamList = consumerStreamsMap.get(topic)
    for(stream <- streamList.asScala; aStream <- stream) {
        println("Message from Single Topic :: " + stream)
    }

    if(consumer != null) {
      consumer.shutdown()
    }
  }

  private def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "500")
    props.put("zookeeper.sync.time.ms", "250")
    props.put("auto.commit.interval.ms", "1000")
    new ConsumerConfig(props)
  }
}
