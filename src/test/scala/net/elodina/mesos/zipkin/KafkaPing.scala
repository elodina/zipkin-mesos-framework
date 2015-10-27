package net.elodina.mesos.zipkin

import java.util.{UUID, Properties}

import com.twitter.zipkin.common.{Span, Annotation, Endpoint}
import com.twitter.zipkin.receiver.kafka.SpanCodec
import kafka.message.NoCompressionCodec
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import org.junit.Test

class KafkaPing {

  lazy val KAFKA_BROKER = Option(System.getenv("KAFKA_BROKER")).getOrElse("localhost:9092")
  lazy val KAFKA_TOPIC = Option(System.getenv("KAFKA_TOPIC")).getOrElse("zipkin")
  lazy val ANN_VALUE = Option(System.getenv("ANN_VALUE"))

  @Test
  def pingTest() {
    val props = new Properties()

    val codec = NoCompressionCodec.codec

    props.put("compression.codec", codec.toString)
    props.put("producer.type", "sync")
    props.put("metadata.broker.list", KAFKA_BROKER)
    props.put("batch.num.messages", "200")
    props.put("message.send.max.retries", "3")
    props.put("request.required.acks", "-1")
    props.put("client.id", UUID.randomUUID().toString)

    val producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))
    val message = createMessage()
    val data = new KeyedMessage(KAFKA_TOPIC, "any".getBytes, message)

    producer.send(data)
    producer.close()
  }

  def createMessage(): Array[Byte] = {
    val annotation = Annotation(System.currentTimeMillis(),
      List(Some("pong"), ANN_VALUE, Some(UUID.randomUUID().toString)).flatten.mkString("-"), Some(Endpoint((127 << 24 | 1), 80, "ping")))
    val message = Span(1, "methodCall", 1, None, List(annotation), Nil)
    val codec = new SpanCodec()
    codec.encode(message)
  }
}
