package be.axxes.streamingdemo.producer

import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.domain.stream.Played
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class SongProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks","all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    for(i <- 0 until 100) {

      val song = Played(i, "hardwell", "My Hero", "Foo fighters", "Paramore")

      val serializedSong = new ObjectMapper().writeValueAsString(song)

      val fut = producer.send(new ProducerRecord("playlist", serializedSong))
      fut.get(1000, TimeUnit.MILLISECONDS)
      Thread.sleep(1000)
    }
  }
}
