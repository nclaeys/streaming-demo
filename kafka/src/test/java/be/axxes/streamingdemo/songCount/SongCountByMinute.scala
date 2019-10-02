package be.axxes.streamingdemo.songCount

import be.axxes.streamingdemo.domain.stream.Played
import be.axxes.streamingdemo.{Avro4Serde, KafkaFactory, StreamTest, StreamingApp}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.test.OutputVerifier

class SongCountByMinute extends StreamTest {

  override def buildTopology(builder: StreamsBuilder): Topology = {
    StreamingApp.createSongCountByWindowTopology(builder, 60 * 1000)
  }

  test("2 times the same song within a minute should produce 2 for the window") {
    val factory: KafkaFactory[String, Played] = createKafkaFactory(10000)

    testDriver.pipeInput(factory.value(Played(0, "customer1", "My Hero", "Foo fighters", "The colour and the shape")))
    testDriver.pipeInput(factory.value(Played(1, "customer2", "My Hero", "Foo fighters", "The colour and the shape")))

    verifyOutput("My Hero#0_60000", 1L)
    verifyOutput("My Hero#0_60000", 2L)
  }


  test("2 times the same song outside a minute should produce count 1 for each window") {
    val factory: KafkaFactory[String, Played] = createKafkaFactory(65000)

    testDriver.pipeInput(factory.value(Played(0, "customer1", "Everlong", "Foo fighters", "The colour and the shape")))
    testDriver.pipeInput(factory.value(Played(1, "customer2", "Everlong", "Foo fighters", "The colour and the shape")))

    verifyOutput("Everlong#0_60000", 1L)
    verifyOutput("Everlong#60000_120000", 1L)
  }

  test("2 times the different song within a minute should produce count 1 for each song") {
    val factory: KafkaFactory[String, Played] = createKafkaFactory(10000)

    testDriver.pipeInput(factory.value(Played(0, "customer1", "Learn to fly", "Foo fighters", "There is nothing left to lose")))
    testDriver.pipeInput(factory.value(Played(1, "customer2", "The pretender", "Foo fighters", "Echoes, silence, patience and grace")))

    verifyOutput("Learn to fly#0_60000", 1L)
    verifyOutput("The pretender#0_60000", 1L)
  }

  def verifyOutput(key: String, value: java.lang.Long) = {
    val stringDeserializer = new StringDeserializer()
    val longDeserializer = new LongDeserializer()

    val output = testDriver.readOutput("songs-played-by-minute-kafka", stringDeserializer, longDeserializer)
    OutputVerifier.compareKeyValue(output, key, value)
  }

  def createKafkaFactory(autoAdvanceMs: Int) = {
    import Avro4Serde._
    import org.apache.kafka.streams.scala.Serdes._
    new KafkaFactory[String, Played]("songs-played", 0, autoAdvanceMs)
  }

}
