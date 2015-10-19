package net.elodina.mesos.zipkin.storage

import java.util.concurrent.CopyOnWriteArrayList

import net.elodina.mesos.zipkin.Config
import net.elodina.mesos.zipkin.components.{WebService, QueryService, Collector}
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.collection.mutable
import scala.collection.JavaConversions._

case class Cluster(_collectors: List[Collector] = Nil,
                   _webServices: List[WebService] = Nil,
                   _queryServices: List[QueryService] = Nil) {

  private[zipkin] var frameworkId: Option[String] = None

  private val storage = Cluster.newStorage(Config.storage)

  private[zipkin] val collectors: mutable.Buffer[Collector] = new CopyOnWriteArrayList[Collector]()
  private[zipkin] val queryServices: mutable.Buffer[QueryService] = new CopyOnWriteArrayList[QueryService]()
  private[zipkin] val webServices: mutable.Buffer[WebService] = new CopyOnWriteArrayList[WebService]()

  collectors ++ _collectors
  queryServices ++ _queryServices
  webServices ++ _webServices

  def clear(): Unit = {
    collectors.clear()
    queryServices.clear()
    webServices.clear()
  }

  def save() = storage.save(this)(Cluster.writer)

  def load() {
    storage.load(Cluster.reader).foreach { cluster =>
      this.frameworkId = cluster.frameworkId
      collectors ++ cluster.collectors
      webServices ++ cluster.webServices
      queryServices ++ cluster.queryServices
    }
  }

  def fetchAllComponents = {
    collectors ++ queryServices ++ webServices
  }

  def isReconciling:Boolean = {
    collectors.exists(_.isReconciling) || queryServices.exists(_.isReconciling) || webServices.exists(_.isReconciling)
  }
}

object Cluster {

  private def newStorage(storage: String): Storage[Cluster] = {
    storage.split(":", 2) match {
      case Array("file", fileName) => FileStorage(fileName)
      case Array("zk", zk) => ZkStorage(zk)
      case _ => throw new IllegalArgumentException(s"Unsupported storage: $storage")
    }
  }

  implicit val writer = new Writes[Cluster] {
    override def writes(o: Cluster): JsValue = {
      Json.obj(
        "frameworkId" -> o.frameworkId,
        "collectors" -> o.collectors.toList,
        "webServices" -> o.webServices.toList,
        "queryServices" -> o.queryServices.toList
      )
    }
  }

  implicit val reader = (
    (__ \ 'frameworkId).readNullable[String] and
    (__ \ 'collectors).read[List[Collector]] and
    (__ \ 'webServices).read[List[WebService]] and
    (__ \ 'queryServices).read[List[QueryService]]
    )((frameworkId, collectors, webServices, queryServices) => {
    val cluster = Cluster(collectors, webServices, queryServices)
    cluster.frameworkId = frameworkId
    cluster
  })
}