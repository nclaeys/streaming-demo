package be.axxes.streamingdemo

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.domain.Customer
import be.axxes.streamingdemo.domain.stream.{Played, PlayedWithCustomerInfo}
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

object GroupByCountryApp {

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "songs-played")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val builder: StreamsBuilder = new StreamsBuilder
    val topology = createSongCountByLocationTopology(builder)

    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(100, TimeUnit.SECONDS)
    }
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
      .peek((key,value) => println(s"---output key: $key and value: $value"))
      .to("songs-played-by-country")

    builder.build()
  }
}
