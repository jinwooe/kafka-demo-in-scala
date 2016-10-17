import java.util.{Date, Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by jadeim on 2016. 10. 17..
  */
object CustomPartitionProducer {
  var producer: KafkaProducer[String, String] = _

  def main(args: Array[String]): Unit = {
    val argsCount = args.length
    if(argsCount == 0 || argsCount == 1) {
      throw new IllegalArgumentException("Please provide topic name and Message count as arguments")
    }

    val topic = args(0)
    val count = args(1)
    val messageCount = Integer.parseInt(count)
    println("Topic Name - " + topic)
    println("Message Count - " + messageCount)
    val simpleProducer = new CustomPartitionProducer()
    simpleProducer.publishMessage(topic, messageCount)
  }
}

class CustomPartitionProducer {
  private var producer: KafkaProducer[String, String] = _
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("request.required.acks", "1")

  producer = new KafkaProducer(props)

  private def publishMessage(topic: String, messageCount: Int) {
    val random = new Random()
    for(count <- 0 until messageCount) {
      val clientIP = "192.168.14." + random.nextInt(255)
      val accessTime = new Date().toString
      val msg = accessTime + ",kafka.apache.org," + clientIP
      println(msg)
      val data = new ProducerRecord[String, String](topic, clientIP, msg)
      producer.send(data)
    }

    producer.close()
  }
}
