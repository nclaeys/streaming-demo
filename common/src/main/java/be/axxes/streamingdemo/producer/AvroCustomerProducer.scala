package be.axxes.streamingdemo.producer

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import be.axxes.streamingdemo.Avro4Serde
import be.axxes.streamingdemo.domain.Customer
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.util.Random

object AvroCustomerProducer {

  def main(args: Array[String]): Unit = {
    createTopicIfNotExists()

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)

    Thread.sleep(10000)
    val producer = new KafkaProducer[String, Array[Byte]](props)
    val rand = new Random()
    for (i <- 0 until 1000) {
      val serde = Avro4Serde.genericSerde[Customer]

      val customer = getCustomer(rand.nextInt(9))

      val fut = producer.send(new ProducerRecord("customers", serde.serializer().serialize("customers", customer)))
      fut.get(1000, TimeUnit.MILLISECONDS)
      Thread.sleep(1000)
    }
  }

  def getCustomer(index: Int) = {
    Seq(
      Customer("Michel", index.toString, "Belgium"),
      Customer("Pedro", index.toString, "Spain"),
      Customer("Boris", index.toString, "UK"),
      Customer("Trump", index.toString, "US"),
      Customer("Merkel", index.toString, "Germany"),
      Customer("Rutten", index.toString, "The Netherlands"),
      Customer("Macron", index.toString, "France"),
      Customer("Putin", index.toString, "Russia"),
      Customer("Guiseppe", index.toString, "Italy"))(index)
  }

  def createTopicIfNotExists() = {
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val adminClient = AdminClient.create(props)
    val deletedTopics = new util.ArrayList[String]()
    deletedTopics.add("customers")
    adminClient.deleteTopics(deletedTopics)

    val createdTopics = new util.ArrayList[NewTopic]()
    createdTopics.add(new NewTopic("customers", 1, 1))
    val result = adminClient.createTopics(createdTopics)
    result.all()
    println(result)
  }
}
