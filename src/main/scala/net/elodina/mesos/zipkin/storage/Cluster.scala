package net.elodina.mesos.zipkin.storage

import net.elodina.mesos.zipkin.Config
import net.elodina.mesos.zipkin.zipkin.{ZipkinComponent, WebService, QueryService, Collector}
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

case class Cluster(_collectors: List[Collector] = Nil,
                   _webServices: List[WebService] = Nil,
                   _queryServices: List[QueryService] = Nil) {

  private[zipkin] var frameworkId: Option[String] = None

  private val storage = Cluster.newStorage(Config.storage)

  private val collectors: ListBuffer[Collector] = new ListBuffer[Collector]()
  private val queryServices: ListBuffer[QueryService] = new ListBuffer[QueryService]()
  private val webServices: ListBuffer[WebService] = new ListBuffer[WebService]()

  collectors ++ _collectors
  queryServices ++ _queryServices
  webServices ++ _webServices

  def getCollectors: List[Collector] = collectors.toList

  def getWebServices: List[WebService] = webServices.toList

  def getQueryServices: List[QueryService] = queryServices.toList

  def getCollector(id: String): Option[Collector] = collectors.find(_.id == id)

  def getWebService(id: String): Option[WebService] = webServices.find(_.id == id)

  def getQueryService(id: String): Option[QueryService] = queryServices.find(_.id == id)

  def addCollector(collector: Collector): Collector = addComponent(collector, collectors)

  def addWebService(webService: WebService): WebService = addComponent(webService, webServices)

  def addQueryService(queryService: QueryService): QueryService = addComponent(queryService, queryServices)

  def removeCollector(collector: Collector): Unit = collectors.remove(collector)

  def removeWebService(webService: WebService): Unit = collectors.remove(webService)

  def removeQueryService(queryService: QueryService): Unit = collectors.remove(queryService)

  private def addComponent[E <: ZipkinComponent](zipkinComponent: E, componentList: mutable.Buffer[E]): E = {
    componentList.add(zipkinComponent)
    zipkinComponent
  }

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