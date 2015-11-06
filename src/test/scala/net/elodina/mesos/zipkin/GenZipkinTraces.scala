package net.elodina.mesos.zipkin

import java.util.Properties

import com.github.kristofa.brave.TraceInfo
import org.junit.Test
import com.github.kristofa.brave.KafkaZipkinTracing._

class GenZipkinTraces {

  lazy val KAFKA_BROKER = Option(System.getenv("KAFKA_BROKER")).getOrElse("localhost:9092")
  lazy val KAFKA_TOPIC = Option(System.getenv("KAFKA_TOPIC")).getOrElse("zipkin")

  @Test
  def generate(): Unit = {
    //Setting producer type to sync in this test, as we don't bother about throughput here, just want all messages sent
    val kafkaProps = new Properties()
    kafkaProps.setProperty("producer.type", "sync")

    initTracing(KAFKA_BROKER, "serviceFoo", Some(KAFKA_TOPIC), kafkaProps = kafkaProps)

    var traceInfo: Option[TraceInfo] = None

    // Logging fire and forget request
    withClientTracer { ct =>
      Option(ct.startNewSpan("someRequest")).foreach { traceIds =>
        // The below line emulates adding trace info to RPC call
        traceInfo = Some(TraceInfo(Some(traceIds.getTraceId), Some(traceIds.getSpanId), sampled = true))
        ct.setClientSent()
        submitClientTracer()
      }
    }

    // Emulate RPC call
    try {
      Thread.sleep(1500)
    } catch {
      case e: InterruptedException => //ignore
    }

    // Creating new tracing for server side
    initTracing(KAFKA_BROKER, "serviceBar", Some(KAFKA_TOPIC), kafkaProps = kafkaProps)

    // Here actual servers should parse TraceInfo instances from an incoming request
    initServerFromTraceInfo(traceInfo)

    withServerTracer { st =>
      st.setServerReceived()
    }

    // Now receiving service is making request of his own
    withClientTracer { ct =>
      Option(ct.startNewSpan("otherRequest")).foreach { traceIds =>
        ct.setClientSent()
        submitClientTracer()
      }
    }

    withServerTracer { st =>
      st.submitAnnotation("Completed Processing")
    }

    submitServerTracer()
  }
}
