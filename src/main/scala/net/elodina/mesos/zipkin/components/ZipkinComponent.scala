package net.elodina.mesos.zipkin.components

import java.util.{UUID, Date}
import net.elodina.mesos.zipkin.utils.{Util, Period, Range}
import net.elodina.mesos.zipkin.components.ZipkinComponent.Task
import org.apache.mesos.Protos.Offer
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.collection.{mutable, Map}
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

case class TaskConfig(var cpus: Double = 1, var mem: Double = 256, var ports: List[Range] = Nil,
                      var envVariables: Map[String, String] = Map(), var flags: Map[String, String] = Map(),
                      var configFile: Option[String] = None)

object TaskConfig {

  implicit val reader = (
      (__ \ 'cpu).read[Double] and
      (__ \ 'mem).read[Double] and
      (__ \ 'ports).read[String].map(Range.parseRanges) and
      (__ \ 'envVariables).read[Map[String, String]] and
      (__ \ 'flags).read[Map[String, String]] and
      (__ \ 'configFile).readNullable[String])(TaskConfig.apply _)

  implicit val writer = new Writes[TaskConfig] {
    def writes(tc: TaskConfig): JsValue = {
      Json.obj(
        "cpu" -> tc.cpus,
        "mem" -> tc.mem,
        "ports" -> tc.ports.mkString(","),
        "envVariables" -> tc.envVariables.toMap,
        "flags" -> tc.flags.toMap,
        "configFile" -> tc.configFile
      )
    }
  }
}

sealed abstract class ZipkinComponent(val id: String = "0") {

  var state: State = Added
  private[zipkin] val constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]
  @volatile var task: Task = null
  var config = setDefaultConfig()

  abstract def setDefaultConfig(): TaskConfig

  abstract def componentName: String

  def nextTaskId: String = s"zipkin-$componentName-$id-${UUID.randomUUID()}"

  def isReconciling: Boolean = this.state == Reconciling

  def matches(offer: Offer, now: Date = new Date(), otherAttributes: String => List[String] = _ => Nil): Option[String] = {

    val offerResources = offer.getResourcesList.toList.map(res => res.getName -> res).toMap

    if (getPort(offer).isEmpty) return Some("no suitable port")

    offerResources.get("cpus") match {
      case Some(cpusResource) => if (cpusResource.getScalar.getValue < config.cpus) return Some(s"cpus ${cpusResource.getScalar.getValue} < ${config.cpus}")
      case None => return Some("no cpus")
    }

    offerResources.get("mem") match {
      case Some(memResource) => if (memResource.getScalar.getValue < config.mem) return Some(s"mem ${memResource.getScalar.getValue} < ${config.mem}")
      case None => return Some("no mem")
    }

    val offerAttributes = offer.getAttributesList.toList.foldLeft(Map("hostname" -> offer.getHostname)) { case (attributes, attribute) =>
      if (attribute.hasText) attributes.updated(attribute.getName, attribute.getText.getValue)
      else attributes
    }

    for ((name, constraints) <- constraints) {
      for (constraint <- constraints) {
        offerAttributes.get(name) match {
          case Some(attribute) => if (!constraint.matches(attribute, otherAttributes(name))) return Some(s"$name doesn't match $constraint")
          case None => return Some(s"no $name")
        }
      }
    }

    None
  }

  private[zipkin] def getPort(offer: Offer): Option[Long] = {
    val ports = Util.getRangeResources(offer, "ports").map(r => Range(r.getBegin.toInt, r.getEnd.toInt))

    if (this.config.ports == Nil) ports.headOption.map(_.start)
    else ports.flatMap(range => this.config.ports.flatMap(range.overlap)).headOption.map(_.start)
  }

  def waitFor(state: State, timeout: Duration): Boolean = {
    var t = timeout.toMillis
    while (t > 0 && this.state != state) {
      val delay = Math.min(100, t)
      Thread.sleep(delay)
      t -= delay
    }

    this.state == state
  }

  def idFromTaskId(taskId: String): String = {
    val parts: Array[String] = taskId.split("-")
    if (parts.length < 3) throw new IllegalArgumentException(taskId)
    parts(2)
  }
}

object ZipkinComponent {

  case class Task(id: String, slaveId: String, executorId: String, attributes: Map[String, String])

  object Task {
    implicit val writer = Json.writes[Task]
    implicit val reader = Json.reads[Task]
  }

  def writeJson[E <: ZipkinComponent](zc: E): JsValue = {
    Json.obj(
      "id" -> zc.id,
      "state" -> zc.state.toString,
      "constraints" -> Util.formatConstraints(zc.constraints),
      "task" -> Option(zc.task),
      "config" -> zc.config
    )
  }

  def configureInstance[E <: ZipkinComponent](zc: E, task: Option[Task], constraints: Map[String, List[Constraint]],
                                              state: String, config: TaskConfig): E = {
    state match {
      case "Added" => zc.state = Added
      case "Stopped" => zc.state = Stopped
      case "Staging" => zc.state = Staging
      case "Running" => zc.state = Running
      case "Reconciling" => zc.state = Reconciling
    }
    zc.task = task.orNull
    constraints.foreach(zc.constraints += _)
    zc.config.cpus = config.cpus
    zc.config.mem = config.mem
    zc.config.ports = config.ports
    zc.config.flags = config.flags
    zc.config.envVariables = config.envVariables
    zc
  }

  def readJson = (__ \ 'id).read[String] and
    (__ \ 'task).readNullable[Task] and
    (__ \ 'constraints).read[String].map(Constraint.parse) and
    (__ \ 'state).read[String] and
    (__ \ 'config).read[TaskConfig]
}

case class Collector(override val id: String = "0") extends ZipkinComponent(id) {

  override def componentName = "collector"

  override def setDefaultConfig(): TaskConfig = {
    TaskConfig(1, 256, Nil, Map(), Map(), Some("collector-dev.scala"))
  }
}


case class QueryService(override val id: String = "0") extends ZipkinComponent(id) {

  override def componentName = "query"

  override def setDefaultConfig(): TaskConfig = {
    TaskConfig(1, 256, Nil, Map(), Map(), Some("query-dev.scala"))
  }
}

case class WebService(override val id: String = "0") extends ZipkinComponent(id) {

  override def componentName = "web"

  override def setDefaultConfig(): TaskConfig = {
    TaskConfig(1, 256)
  }
}

object Collector {
  implicit val writer = new Writes[Collector] {
    def writes(zc: Collector): JsValue = ZipkinComponent.writeJson(zc)
  }

  implicit val reader = ZipkinComponent.readJson((id, task, constraints, state, config) => {
    ZipkinComponent.configureInstance(Collector(id), task, constraints, state, config)
  })
}

object QueryService {
  implicit val writer = new Writes[QueryService] {
    def writes(zc: QueryService): JsValue = ZipkinComponent.writeJson(zc)
  }

  implicit val reader = ZipkinComponent.readJson((id, task, constraints, state, config) => {
    ZipkinComponent.configureInstance(QueryService(id), task, constraints, state, config)
  })
}

object WebService {
  implicit val writer = new Writes[WebService] {
    def writes(zc: WebService): JsValue = ZipkinComponent.writeJson(zc)
  }

  implicit val reader = ZipkinComponent.readJson((id, task, constraints, state, config) => {
    ZipkinComponent.configureInstance(WebService(id), task, constraints, state, config)
  })
}