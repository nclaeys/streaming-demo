package be.axxes.streamingdemo.producer

import java.nio.file.{Files, Paths}
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.Avro4Serde
import be.axxes.streamingdemo.domain.Song
import be.axxes.streamingdemo.domain.stream.Played
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.io.Source
import scala.util.Random

object AvroSongProducer {

  def main(args: Array[String]): Unit = {
    createTopicIfNotExists()

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    //props.put("schema.registry.url", "http://localhost:8081")
    //props.put("auto.register.schemas", "false")

    Thread.sleep(10000)
    val producer = new KafkaProducer[String, Array[Byte]](props)
    for (i <- 0 until 1000) {
      val serde = Avro4Serde.genericSerde[Played]


      val played = getPlayedEntry(i)

      val fut = producer.send(new ProducerRecord("songs-played", serde.serializer().serialize("songs-played", played)))
      fut.get(1000, TimeUnit.MILLISECONDS)
      Thread.sleep(1000)
    }
  }

  def createTopicIfNotExists() = {
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val adminClient = AdminClient.create(props)
    val deletedTopics = new util.ArrayList[String]()
    deletedTopics.add("songs-played")
    adminClient.deleteTopics(deletedTopics)

    val createdTopics = new util.ArrayList[NewTopic]()
    createdTopics.add(new NewTopic("songs-played", 1, 1))
    val result = adminClient.createTopics(createdTopics)
    result.all()
    println(result)
  }

  private def getPlayedEntry(index: Int): Played = {
    val rand = new Random()
    val song = getSongs(rand.nextInt(9))

    val customer = getCustomers(rand.nextInt(9))
    Played(index, customer.toString, song.title, song.artist, song.album)
  }

  private def getCustomers(index : Int) = {
    index / 10
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
