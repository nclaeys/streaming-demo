package be.axxes.streamingdemo

import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.domain.{Customer, PlayedWithCustomerInfo}
import be.axxes.streamingdemo.domain.stream.Played
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

object StreamingApp {

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "songs-played")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092")
    val builder: StreamsBuilder = new StreamsBuilder
    val topology = createSongCountByWindowTopology(builder, 60 * 1000)

    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
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
      .windowedBy(TimeWindows.of(windowInMillis))
      .count()
      .toStream
      .map { (key, value) => (printWindowedKey(key), value) }
      .to("songs-played-by-minute")

    builder.build()
  }

  def createSongCountByLocationTopology(builder: StreamsBuilder): Topology = {
    import Avro4Serde._
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._

    val latestInfoByCustomerId: KTable[String, Customer] = builder.stream[String, Customer]("customers")
      .map((k, v) => (v.id, v)).groupByKey.reduce((v1, v2) => v2)

    builder.stream[String, Played]("songs-played")
      .map((k, v) => (v.customerId, v))
      .leftJoin(latestInfoByCustomerId)((played, customerInfo) => customerInfo match {
        case null => PlayedWithCustomerInfo(played.customerId, "unknown", played.title, played.artist, "unknown")
        case e => PlayedWithCustomerInfo(customerInfo.id, customerInfo.country, played.title, played.artist, customerInfo.name)
      })
      .groupBy((_, v) => v.country)
      .count()
      .toStream
      .to("songs-played-by-country")

    builder.build()
  }

  private def printWindowedKey(key: Windowed[String]) = {
    key.key() + "#" + key.window().start() + "_" + key.window().end()
  }
}
