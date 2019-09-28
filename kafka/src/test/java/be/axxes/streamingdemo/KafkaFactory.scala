package be.axxes.streamingdemo

import java.time.Instant

import org.apache.kafka.common.serialization.{Serde, Serializer}
import org.apache.kafka.streams.test.ConsumerRecordFactory

class KafkaFactory[K, V](
                     defaultTopicName: String,
                     keySerializer: Serializer[K],
                     valueSerializer: Serializer[V],
                     startTimestampMs: Long = System.currentTimeMillis,
                     autoAdvanceMs: Long = 0L
                   ) {

  def this(defaultTopicName: String)(implicit keySerde: Serde[K], valueSerde: Serde[V]) {
    this(defaultTopicName, keySerde.serializer(), valueSerde.serializer())
  }

  def this(defaultTopicName: String, startTimestampMs: Long, autoAdvanceMs: Long)(implicit keySerde: Serde[K], valueSerde: Serde[V]) {
    this(defaultTopicName, keySerde.serializer(), valueSerde.serializer(), startTimestampMs, autoAdvanceMs)
  }

  val inner: ConsumerRecordFactory[K, V] = new ConsumerRecordFactory[K, V](defaultTopicName, keySerializer, valueSerializer, startTimestampMs, autoAdvanceMs)

  def value(v: V) = inner.create(v)

  def valueInstant(v: V, ts: Instant) = inner.create(v, ts.toEpochMilli)

  def keyValue(k: K, v: V) = inner.create(k, v)

  def keyValueInstant(k: K, v: V, ts: Instant) = inner.create(k, v, ts.toEpochMilli)

  def topicValue(topic: String, v: V) = inner.create(topic, v)

  def topicKeyValue(topic: String, k: K, v: V) = inner.create(topic, k, v)

  def topicKeyValueInstant(topic: String, k: K, v: V, ts: Instant) = inner.create(topic, k, v, ts.toEpochMilli)
}
