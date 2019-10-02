package be.axxes.streamingdemo

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.domain.Customer
import be.axxes.streamingdemo.domain.stream.{Played, PlayedWithCustomerInfo}
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

object GroupByWindowApp {

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "songs-played")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val builder: StreamsBuilder = new StreamsBuilder
    val topology = createSongCountByWindowTopology(builder, 5 * 1000)

    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(100, TimeUnit.SECONDS)
    }
  }

  def createSongCountByWindowTopology(builder: StreamsBuilder, windowInMillis: Long): Topology = {
    import Avro4Serde._
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._
    val songsPlayed = builder.stream[String, Played]("songs-played")

    songsPlayed
      .mapValues(played => played.title)
      .groupBy((_, v) => v)
      .windowedBy(TimeWindows.of(Duration.ofMillis(windowInMillis)))
      .count()(Materialized.`with`(Serdes.String,Serdes.Long))
      .toStream
      .map { (key, value) => (printWindowedKey(key), value) }
      .peek{ (s,l) => println("---output: " + s + " with value " + l)}
      .to("songs-played-by-minute-kafka")

    builder.build()
  }

  private def printWindowedKey(key: Windowed[String]) = {
    s"${key.key()}#${key.window().start()}_${key.window().end()}"
  }
}
