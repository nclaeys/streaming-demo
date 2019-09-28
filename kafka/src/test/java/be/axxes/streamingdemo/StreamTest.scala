package be.axxes.streamingdemo

import java.time.Instant
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{StreamsConfig, Topology, TopologyTestDriver}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}

trait StreamTest extends FunSuite with Matchers with BeforeAndAfter {

  def buildTopology(builder: StreamsBuilder): Topology

  def buildTestProps(): Properties = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test" + Instant.now.toEpochMilli)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    props
  }

  def topology = buildTopology(new StreamsBuilder())
  def props = buildTestProps()

  var testDriver: TopologyTestDriver = new TopologyTestDriver(topology, props)

  before {
    testDriver = new TopologyTestDriver(topology, props)
    println("before executed")
  }

  after {
    testDriver.close()
    println("after executed")
  }
}
