import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner}
import org.apache.kafka.common.Cluster

/**
  * Created by jadeim on 2016. 10. 17..
  */
object SimplePartitioner {
  private var producer: KafkaProducer[String, String] = _
}

class SamplePartitioner extends Partitioner {

  def partition(key: Any, numPartitions: Int): Int = {
    var partition = 0
    val partitionKey = key.asInstanceOf[String]
    val offset = partitionKey.lastIndexOf('.')
    if(offset > 0) {
      partition = Integer.parseInt(partitionKey.substring(offset + 1)) % numPartitions
    }
    partition
  }

  override def close() {

  }

  override def partition(topic: String,
                         key: scala.Any,
                         keyBytes: Array[Byte],
                         value: scala.Any,
                         valueBytes: Array[Byte],
                         cluster: Cluster): Int = partition(key, 10)

  override def configure(configs: util.Map[String, _]) {

  }
}
