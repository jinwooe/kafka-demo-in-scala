/**
  * Created by jadeim on 2016. 10. 17..
  */

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SimpleProducer {


  def main(args: Array[String]): Unit = {
    val argsCount = args.length
    if (argsCount == 0 || argsCount == 1)
      throw new IllegalArgumentException("Provide topic name and Message count as arguments")

    val topic = args(0)
    val count = args(1)
    val messageCount = Integer.parseInt(count)
    println("Topic Name - " + topic)
    println("Message Count - " + messageCount)
    val simpleProducer = new SimpleProducer()
    simpleProducer.publishMessage(topic, messageCount)
  }
}

class SimpleProducer {
  private var producer: KafkaProducer[String, String] = _
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("request.required.acks", "1")

  producer = new KafkaProducer(props)

  private def publishMessage(topic: String, messageCount: Int) {
    for(mCount <- 0 until messageCount) {
      val runtime = new Date().toString
      val msg = "Message Publishing Time - " + runtime
      println(msg)

      val data = new ProducerRecord[String, String](topic, msg)
      producer.send(data)
    }

    producer.close()
  }

}
