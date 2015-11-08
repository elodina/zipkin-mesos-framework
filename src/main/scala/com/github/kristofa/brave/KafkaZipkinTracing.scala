package com.github.kristofa.brave

import java.io.{ByteArrayOutputStream, UnsupportedEncodingException}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.CopyOnWriteArraySet
import java.util.{Properties, UUID}

import com.twitter.zipkin.gen.{AnnotationType, BinaryAnnotation, Span}
import kafka.javaapi.producer.Producer
import kafka.message.NoCompressionCodec
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.log4j.Logger
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TIOStreamTransport

import scala.collection.JavaConversions._

object KafkaZipkinTracing {

  private val logger = Logger.getLogger(KafkaZipkinTracing.this.getClass)
  private var brave: Option[Brave] = None

  private val DEFAULT_KAFKA_TOPIC = "zipkin"

  def initTracing(brokerList: String,
                  serviceName: String,
                  topic: Option[String] = None,
                  sampleRate: Option[Int] = None,
                  port: Option[Int] = None,
                  hostname: Option[String] = None,
                  kafkaProps: Properties = new Properties()): Unit = {
    val props = new Properties()
    props.put("metadata.broker.list",
      kafkaProps.getProperty("metadata.broker.list", brokerList))
    props.put("request.required.acks",
      kafkaProps.getProperty("request.required.acks", "1"))
    props.put("producer.type",
      kafkaProps.getProperty("producer.type", "async"))
    props.put("compression.codec",
      kafkaProps.getProperty("compression.codec", NoCompressionCodec.codec.toString))
    props.put("message.send.max.retries",
      kafkaProps.getProperty("message.send.max.retries", "3"))
    props.put("batch.num.messages",
      kafkaProps.getProperty("batch.num.messages", "200"))
    props.put("client.id",
      kafkaProps.getProperty("client.id", UUID.randomUUID().toString))

    val spanCollector = KafkaSpanCollector(
      new ProducerConfig(props),
      topic.getOrElse(DEFAULT_KAFKA_TOPIC)
    )
    brave = Some(new Brave.Builder()
      .spanCollector(spanCollector)
      .traceFilters(List(new FixedSampleRateTraceFilter(sampleRate.getOrElse(1))))
      .traceState(new ThreadLocalServerAndClientSpanState(InetAddress.getLocalHost, port.getOrElse(0), serviceName))
      .build())
  }

  private def withBrave(action: Brave => Unit): Unit = {
    brave.fold(logger.warn("Brave tracing instance accessed before being initialized. Ignoring tracing action."))(action)
  }

  def withClientTracer(action: ClientTracer => Unit): Unit = {
    withBrave { brave =>
      brave.serverTracer()
      action(brave.clientTracer())
      brave.clientTracer()
    }
  }

  def withServerTracer(action: ServerTracer => Unit): Unit = {
    withBrave { brave => action(brave.serverTracer()) }
  }

  /**
   * Initiates tracing based on the data sent by client service
   *
   * @param traceInfo trace info that may be provided from the client service
   * @param initNewTrace indicates whether we want to initiate new trace in case if trace info we got is absent or incomplete
   */
  def initServerFromTraceInfo(traceInfo: Option[TraceInfo] = None, initNewTrace: Boolean = false, spanName: String = "unknown_request"): Unit = {
    def onIncompleteTraces = if (initNewTrace) {
      { st: ServerTracer => st.setStateUnknown(spanName) }
    } else {
      { st: ServerTracer => st.setStateNoTracing() }
    }

    withServerTracer { st =>
      st.clearCurrentSpan()
      traceInfo.fold(onIncompleteTraces(st))({ ti =>
        if (ti.sampled) {
          (for {
            defSpanId <- ti.spanId
            defTraceId <- ti.traceId
          } yield {
              st.setStateCurrentTrace(defTraceId, defSpanId, ti.parentSpanId.map(_.asInstanceOf[java.lang.Long]).orNull, spanName)
            }).getOrElse(onIncompleteTraces(st))
        } else {
          st.setStateNoTracing()
        }
      })
    }
  }

  def submitClientTracer(): Unit = {
    withClientTracer { ct =>
      val currentSpan: Span = ct.spanAndEndpoint.span
      if (currentSpan != null) {
        ct.spanCollector.collect(currentSpan)
        ct.spanAndEndpoint.state.setCurrentClientSpan(null)
        ct.spanAndEndpoint.state.setCurrentClientServiceName(null)
      }
    }
  }

  def submitServerTracer(): Unit = {
    withServerTracer { st =>
      val currentSpan: Span = st.spanAndEndpoint.state.getCurrentServerSpan.getSpan
      if (currentSpan != null) {
        st.spanCollector.collect(currentSpan)
        st.spanAndEndpoint.state.setCurrentServerSpan(null)
      }
    }
  }
}

case class TraceInfo(traceId: Option[Long] = None,
                     spanId: Option[Long] = None,
                     sampled: Boolean = false,
                     parentSpanId: Option[Long] = None)

// TODO: Kafka producer needs to have callback handler, so we could see if trace sending failed
case class KafkaSpanCollector(producerConfig: ProducerConfig,
                              kafkaTopic: String) extends SpanCollector {

  private val logger = Logger.getLogger(KafkaSpanCollector.this.getClass)
  private val defaultAnnotations = new CopyOnWriteArraySet[BinaryAnnotation]

  val producer = new Producer[Array[Byte], Array[Byte]](producerConfig)

  def collect(span: com.twitter.zipkin.gen.Span) {
    for (ba <- defaultAnnotations) {
      span.addToBinary_annotations(ba)
    }

    sendSpan(span)
  }

  def sendSpan(span: com.twitter.zipkin.gen.Span): Unit = {
    try {
      val baos = new ByteArrayOutputStream()
      val streamProtocol = new TBinaryProtocol.Factory().getProtocol(new TIOStreamTransport(baos))

      if (span != null) {
        baos.reset()
        try {
          span.write(streamProtocol)
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]]("zipkin", baos.toByteArray))
        } catch {
          case e: TException =>
            logger.warn(s"Logging span failed. " +
              s"Span ${span.getName} with id ${span.getId}, trace id ${span.getTrace_id} is lost!", e)
        }
      }
    } catch {
      case e: Exception => logger.warn(s"Couldn't log spans while flushing. Unexpected error occurred", e)
    }
  }

  def addDefaultAnnotation(key: String, value: String): Unit = {
    try {
      val binaryAnnotation: BinaryAnnotation = new BinaryAnnotation(key, ByteBuffer.wrap(value.getBytes("UTF-8")),
        AnnotationType.STRING)
      defaultAnnotations.add(binaryAnnotation)
    } catch {
      case e: UnsupportedEncodingException =>
        throw new IllegalStateException(e)
    }
  }

  def close() {
    producer.close
  }
}
