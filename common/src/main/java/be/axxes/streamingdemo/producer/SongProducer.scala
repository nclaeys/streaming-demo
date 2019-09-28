package be.axxes.streamingdemo.producer

import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.domain.Song
import be.axxes.streamingdemo.domain.stream.Played
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

class SongProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")

    val producer = new KafkaProducer[String, Played](props)
    for (i <- 0 until 100) {

      val song = getPlayedEntry

      val fut = producer.send(new ProducerRecord("playlist", song))
      fut.get(1000, TimeUnit.MILLISECONDS)
      Thread.sleep(1000)
    }
  }

  private def getPlayedEntry = {
    val rand = new Random()
    val song = getSongs(rand.nextInt(9))

    val dj = getDjs(rand.nextInt(9))
    Played(rand.nextInt(9), dj, song.title, song.artist, song.album)
  }

  private def getDjs(index : Int) = {
    Seq("Martin gerrix",
      "Dimitri Vegas & Like mike",
      "Hardwell",
      "Armin van Buruen",
      "David Guetta",
      "Tiesto",
      "Don diablo",
      "Afrojack",
      "Oliver Heldens",
      "Steve Aoki")(index)
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
