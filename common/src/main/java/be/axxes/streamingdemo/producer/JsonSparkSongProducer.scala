package be.axxes.streamingdemo.producer

import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.domain.Song
import be.axxes.streamingdemo.domain.stream.Played
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object JsonSparkSongProducer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    for (i <- 0 until 1000) {

      val song = getPlayedEntry

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val serializedSong = mapper.writeValueAsString(song)

      val fut = producer.send(new ProducerRecord("songs-played-json", serializedSong))
      fut.get(1000, TimeUnit.MILLISECONDS)
      Thread.sleep(1000)
    }
  }

  private def getPlayedEntry = {
    val rand = new Random()
    val song = getSongs(rand.nextInt(9))

    val customerId = rand.nextInt(9)
    Played(rand.nextInt(9), customerId.toString, song.title, song.artist, song.album)
  }


  private def getSongs(index: Int) = {
    Seq(Song(index, "My Hero", "Foo fighters", "The colour and the shape"),
      Song(index, "Learn to fly", "Foo fighters", "There is nothing left to lose"),
      Song(index, "This is a call", "Foo fighters", "Foo figthers"),
      Song(index, "Something from nothing", "Foo fighters", "Sonic highways"),
      Song(index, "Walk", "Foo fighters", "Wasting light"),
      Song(index, "The pretender", "Foo fighters", "Echoes, silence, patience and grace"),
      Song(index, "Monkey wrench", "Foo fighters", "The colour and the shape"),
      Song(index, "Everlong", "Foo fighters", "The colour and the shape"),
      Song(index, "All my life", "Foo fighters", "One by one"),
      Song(index, "Best of you", "Foo fighters", "In your honour"),
    )(index)
  }
}
