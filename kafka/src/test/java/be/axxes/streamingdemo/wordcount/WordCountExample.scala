package be.axxes.streamingdemo.wordcount

import be.axxes.streamingdemo.{Avro4Serde, KafkaFactory, StreamTest}
import org.apache.kafka.common.serialization.{IntegerDeserializer, LongDeserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.test.OutputVerifier

case class Text(elem: List[String])

class WordCountExample extends StreamTest{


  override def buildTopology(builder: StreamsBuilder): Topology = {
    import Avro4Serde._
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._


    builder
      .stream[String, Text]("topic1")
      .flatMap((k, v) => v.elem.map(x => (x, 1)))
      .groupByKey
      .count()
      .toStream
      .to("topic2")

    builder.build()
  }

  val stringSerializer = new StringSerializer

  val stringDeserializer = new StringDeserializer()
  val integerDeserializer = new IntegerDeserializer()
  val longDeserializer = new LongDeserializer()


  def verifyOutput(key: String, value: java.lang.Long) = {
    val output = testDriver.readOutput("topic2", stringDeserializer, longDeserializer)
    OutputVerifier.compareKeyValue(output, key, value)
  }

  test("an added message should appear on the output") {
    import Avro4Serde._
    import org.apache.kafka.streams.scala.Serdes._
    val factory = new KafkaFactory[String, Text]("topic1")

    testDriver.pipeInput(factory.value(Text(List("hello","world", "world"))))
    verifyOutput("hello", 1L)
    verifyOutput("world", 1L)
    verifyOutput("world", 2L)

    testDriver.pipeInput(factory.value(Text(List("hello","world", "world"))))
    verifyOutput("hello", 2L)
    verifyOutput("world", 3L)
    verifyOutput("world", 4L)
  }
}
