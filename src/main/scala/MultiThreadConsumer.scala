import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import kafka.consumer.{Consumer, ConsumerConfig}
import scala.collection.JavaConverters._

import MultiThreadConsumer._

/**
  * Created by jadeim on 2016. 10. 18..
  */
object MultiThreadConsumer {
  private def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "500")
    props.put("zookeeper.sync.time.ms", "250")
    props.put("auto.commit.interval.ms", "1000")
    new ConsumerConfig(props)
  }

  def main(args: Array[String]): Unit = {
    val zookeeper = args(0)
    val groupId = args(1)
    val topic = args(2)
    val threadCount = Integer.parseInt(args(3))
    val multiThreadHLConsumer = new MultiThreadConsumer(zookeeper, groupId, topic)
    multiThreadHLConsumer.testMultiThreadConsumer(threadCount)
    try {
      Thread.sleep(600 * 1000)
    }
    catch {
      case ie: InterruptedException =>
    }
    multiThreadHLConsumer.shutdown()
  }
}

class MultiThreadConsumer(zookeeper: String, groupId: String, topic: String) {
  private var executor: ExecutorService = _

  private val consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId))

  def shutdown(): Unit = {
    if (consumer != null) consumer.shutdown()
    if (executor != null) executor.shutdown()
  }

  def testMultiThreadConsumer(threadCount: Int): Unit = {
    val topicMap = new util.HashMap[String, Integer]()
    topicMap.put(topic, threadCount)

    val consumerStreamsMap = consumer.createMessageStreams(topicMap)
    val streamList = consumerStreamsMap.get(topic)

    executor = Executors.newFixedThreadPool(threadCount)
    var count = 0
    for (stream <- streamList.asScala) {
      val threadNumber = count
      executor.submit(new Runnable() {
        override def run(): Unit = {
          val consumerIte = stream.iterator()
          while(consumerIte.hasNext()) {
            println("Thread Number #" + threadNumber + ": " + new String(consumerIte.next().message()))
            println("Shutting down Thread Number: " + threadNumber)
          }
        }
      })
      count += 1
    }
  }
}
