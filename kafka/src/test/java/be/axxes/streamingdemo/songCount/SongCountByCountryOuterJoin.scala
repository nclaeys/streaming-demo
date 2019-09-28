package be.axxes.streamingdemo.songCount

import be.axxes.streamingdemo.domain.stream.Played
import be.axxes.streamingdemo.domain.{Customer, PlayedWithCustomerInfo}
import be.axxes.streamingdemo.{Avro4Serde, KafkaFactory, StreamTest, StreamingApp}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.test.OutputVerifier

class SongCountByCountryOuterJoin extends StreamTest {

  override def buildTopology(builder: StreamsBuilder): Topology = {
    StreamingApp.createSongCountByLocationTopology(builder)
  }

  test("join song played and customer info exists results in count for correct country") {
    val songsFactory: KafkaFactory[String, Played] = createKafkaFactoryPlayedSongs()
    val customerFactory: KafkaFactory[String, Customer] = createKafkaFactoryCustomerInfo()

    testDriver.pipeInput(customerFactory.value(Customer("Niels", "customer1", "Belgium")))
    testDriver.pipeInput(songsFactory.value(Played(0, "customer1", "My Hero", "Foo fighters", "The colour and the shape")))

    verifyOutput("Belgium", 1L)
  }

  test("join song played and customer info does not exist, no output") {
    val songsFactory: KafkaFactory[String, Played] = createKafkaFactoryPlayedSongs()

    testDriver.pipeInput(songsFactory.value(Played(0, "customer1", "My Hero", "Foo fighters", "The colour and the shape")))

    verifyOutput("unknown", 1L)
  }

  test("join song played and customer info comes in late, no output") {
    val songsFactory: KafkaFactory[String, Played] = createKafkaFactoryPlayedSongs()
    val customerFactory: KafkaFactory[String, Customer] = createKafkaFactoryCustomerInfo()

    testDriver.pipeInput(songsFactory.value(Played(0, "customer1", "My Hero", "Foo fighters", "The colour and the shape")))
    testDriver.pipeInput(customerFactory.value(Customer("Niels", "customer1", "Belgium")))

    verifyOutput("unknown", 1L)
  }

  def verifyOutput(key: String, value: java.lang.Long) = {
    val stringDeserializer = new StringDeserializer()
    val longDeserializer = new LongDeserializer()

    val output = testDriver.readOutput("songs-played-by-country", stringDeserializer, longDeserializer)
    OutputVerifier.compareKeyValue(output, key, value)
  }

  def createKafkaFactoryPlayedSongs() = {
    import Avro4Serde._
    import org.apache.kafka.streams.scala.Serdes._
    new KafkaFactory[String, Played]("songs-played", 0, 10000)
  }

  def createKafkaFactoryCustomerInfo() = {
    import Avro4Serde._
    import org.apache.kafka.streams.scala.Serdes._
    new KafkaFactory[String, Customer]("customers", 0, 10000)
  }

}