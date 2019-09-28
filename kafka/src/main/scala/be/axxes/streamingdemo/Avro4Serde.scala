package be.axxes.streamingdemo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import com.sksamuel.avro4s._
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

case object Avro4Serde {

  implicit def genericSerde[T : SchemaFor : Encoder: Decoder]: Serde[T] = new Serde[T] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
    override def close(): Unit = {}

    override def serializer(): Serializer[T] = new Serializer[T] {

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

      override def close(): Unit = {}

      override def serialize(topic: String, data: T): Array[Byte] = {

        def serializeNotNullData(data: T): Array[Byte] = {
          val baos = new ByteArrayOutputStream()
          val output = AvroOutputStream.data[T].to(baos).build(implicitly[SchemaFor[T]].schema)
          output.write(data)
          output.close()
          baos.toByteArray
        }

        Option(data).map(serializeNotNullData).orNull
      }
    }

    override def deserializer(): Deserializer[T] = new Deserializer[T] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
      override def close(): Unit = {}

      override def deserialize(topic: String, data: Array[Byte]): T = {

        def deserializeNotNull(data: Array[Byte]): T = {
          val in = new ByteArrayInputStream(data)
          val is = AvroInputStream.data[T].from(in).build(implicitly[SchemaFor[T]].schema)
          val input = is.iterator.toSet
          is.close()
          input.head
        }

        Option(data).map(deserializeNotNull).getOrElse(null.asInstanceOf[T])
      }
    }
  }
}
