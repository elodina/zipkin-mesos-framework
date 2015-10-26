package net.elodina.mesos.zipkin.http

import java.io.{FileInputStream, File}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import net.elodina.mesos.zipkin.mesos.Scheduler
import net.elodina.mesos.zipkin.utils.Util
import net.elodina.mesos.zipkin.components._
import org.apache.log4j.Logger
import play.api.libs.json._
import net.elodina.mesos.zipkin.utils.{Range => URange}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import scala.util.Try
import play.api.libs.json.Json._


case class ApiResponse[E <: ZipkinComponent](success: Boolean = true, message: String = "", value: Option[List[E]] = None)

object ApiResponse {

  implicit def apiResponseReads[E <: ZipkinComponent](implicit fmt: Reads[E]): Reads[ApiResponse[E]] = new Reads[ApiResponse[E]] {
    def reads(json: JsValue): JsResult[ApiResponse[E]] = {
      JsSuccess(ApiResponse((json \ "success").as[Boolean],
        (json \ "message").as[String],
        json \ "value" match {
          case JsArray(v) => Some(v.map(t => fromJson(t)(fmt) match {
            case value: JsSuccess[E] => value.get
            case e: JsError => throw new IllegalArgumentException(s"invalid Zipkin instance value supplied: ${JsError.toFlatJson(e).toString()}")
          }).toList)
          case _ => None
        }))
    }
  }

  implicit def apiResponseWrites[E <: ZipkinComponent](implicit fmt: Writes[E]): Writes[ApiResponse[E]] = new Writes[ApiResponse[E]] {
    def writes(ar: ApiResponse[E]) = {
      var res = Seq(
      "success" -> JsBoolean(ar.success),
      "message" -> JsString(ar.message))
      ar.value.foreach(x => res = res :+ ("value" -> JsArray(x.map(toJson(_)))))
      JsObject(res)
    }
  }
}

class Servlet extends HttpServlet {

  val logger = Logger.getLogger(HttpServer.getClass)

  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = doGet(request, response)

  override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val url = request.getRequestURL + (if (request.getQueryString != null) "?" + request.getQueryString else "")
    logger.info("handling - " + url)

    try {
      handle(request, response)
      logger.info("finished handling")
    } catch {
      case e: Exception =>
        logger.error("error handling", e)
        response.sendError(500, "" + e)
    }
  }

  def handle(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val uri = request.getRequestURI

    //handling incoming request
    if (uri.startsWith("/jar/")) downloadFile(HttpServer.jar, response)
    else if (uri.startsWith("/collector/")) downloadFile(HttpServer.collector, response)
    else if (uri.startsWith("/query/")) downloadFile(HttpServer.query, response)
    else if (uri.startsWith("/web/")) downloadFile(HttpServer.web, response)
    else if (uri.startsWith("/collector-conf/")) downloadConfigFile(uri, response, HttpServer.collectorConfigFiles)
    else if (uri.startsWith("/query-conf/")) downloadConfigFile(uri, response, HttpServer.queryConfigFiles)
    else if (uri.startsWith("/web-resources/")) downloadFile(HttpServer.webResources, response)
    else if (uri.startsWith("/api")) handleApi(request, response)
    else response.sendError(404)
  }

  def downloadFile(file: File, response: HttpServletResponse) {
    response.setContentType("application/zip")
    response.setHeader("Content-Length", "" + file.length())
    response.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName + "\"")
    Util.copyAndClose(new FileInputStream(file), response.getOutputStream)
  }

  def downloadConfigFile(uri: String, response: HttpServletResponse, configFiles: List[File]) {
    val confFileName = uri.split("/").last
    configFiles.find(_.getName == confFileName) match {
      case Some(file) => downloadFile(file, response)
      case None => response.sendError(404)
    }
  }

  private def fetchPathPart(request: HttpServletRequest, partToCut: String): String = {
    val uri: String = request.getRequestURI.substring(partToCut.length)
    if (uri.startsWith("/")) uri.substring(1) else uri
  }

  def handleApi(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("application/json; charset=utf-8")

    val uri = fetchPathPart(request, "/api")
    if (uri.startsWith("collector")) handleZipkin(request, response, { Scheduler.cluster.collectors }, {Collector(_)},
    "collector", {(x: Boolean, y: String, z: Option[List[Collector]]) => Json.toJson(ApiResponse[Collector](x, y, z) )})
    else if (uri.startsWith("query")) handleZipkin(request, response, { Scheduler.cluster.queryServices }, { QueryService(_) },
    "query", {(x: Boolean, y: String, z: Option[List[QueryService]]) => Json.toJson(ApiResponse[QueryService](x, y, z) )})
    else if (uri.startsWith("web")) handleZipkin(request, response, { Scheduler.cluster.webServices }, { WebService(_) },
    "web", {(x: Boolean, y: String, z: Option[List[WebService]]) => Json.toJson(ApiResponse[WebService](x, y, z) )})
    else response.sendError(404)
  }

  def handleZipkin[E <: ZipkinComponent](request: HttpServletRequest,
                                         response: HttpServletResponse,
                                         fetchCollection: => collection.mutable.Buffer[E],
                                         instantiateComponent: String => E,
                                         uriPath: String,
                                         createApiResponse: (Boolean, String, Option[List[E]]) => JsValue) {
    logger.info(s"Got parameters: \n${request.getParameterMap.map {pm => s"${pm._1}: ${pm._2.headOption.getOrElse("")}" }.mkString("\n")}")
    val uri = fetchPathPart(request, s"/api/$uriPath")
    if (uri == "list") handleList(request, response, fetchCollection, uriPath, createApiResponse)
    else if (uri == "add") handleAdd(request, response, fetchCollection, instantiateComponent, uriPath, createApiResponse)
    else if (uri == "start") handleStart(request, response, fetchCollection, uriPath, createApiResponse)
    else if (uri == "stop") handleStop(request, response, fetchCollection, uriPath, createApiResponse)
    else if (uri == "remove") handleRemove(request, response, fetchCollection, uriPath, createApiResponse)
    else if (uri == "config") handleConfig(request, response, fetchCollection, uriPath, createApiResponse)
    else response.sendError(404)
  }

  private def handleAdd[E <: ZipkinComponent](request: HttpServletRequest, response: HttpServletResponse,
                                              fetchCollection: => mutable.Buffer[E], instantiateComponent: String => E,
                                              componentName: String, createApiResponse: (Boolean, String, Option[List[E]]) => JsValue) {
    val idExpr = request.getParameter("id")
    val ids = Util.expandIds(idExpr, fetchCollection)
    val cpus = Option(request.getParameter("cpu"))
    val mem = Option(request.getParameter("mem"))
    val constraints = Option(request.getParameter("constraints"))
    val ports = Option(request.getParameter("port"))
    val adminPorts = Option(request.getParameter("adminPort"))
    val flags = Option(request.getParameter("flags"))
    val env = Option(request.getParameter("env"))
    val configFile = Option(request.getParameter("configFile"))
    val existing = ids.filter(id => fetchCollection.exists(_.id == id))
    if (existing.nonEmpty) {
      response.getWriter.println(createApiResponse(false, s"Zipkin $componentName instance(s) ${existing.mkString(",")} already exist", None))
    } else {
      val components = ids.map { id =>
        val component = instantiateComponent(id)
        cpus.foreach(cpus => component.config.cpus = cpus.toDouble)
        mem.foreach(mem => component.config.mem = mem.toDouble)
        ports.foreach(ports => component.config.ports = URange.parseRanges(ports))
        adminPorts.foreach(adminPorts => component.config.adminPorts = URange.parseRanges(adminPorts))
        component.constraints ++= Constraint.parse(constraints.getOrElse(""))
        flags.foreach(flags => component.config.flags = Util.parseMap(flags))
        env.foreach(ev => component.config.env = Util.parseMap(ev))
        configFile.foreach(cf => component.config.configFile = Some(cf))
        fetchCollection += component
        component
      }
      Scheduler.cluster.save()
      response.getWriter.println(createApiResponse(true, s"Added servers $idExpr", Some(components)))
    }
  }

  private def handleStart[E <: ZipkinComponent](request: HttpServletRequest, response: HttpServletResponse,
                                                fetchCollection: => mutable.Buffer[E], componentName: String,
                                                createApiResponse: (Boolean, String, Option[List[E]]) => JsValue) {
    processExistingInstances(request, response, fetchCollection, componentName, createApiResponse, { (ids, idExpr) =>
      val timeout = Duration(Option(request.getParameter("timeout")).getOrElse("60s"))
      val components = ids.flatMap { id =>
        fetchCollection.find(_.id == id).map { component =>
          if (component.state == Added) {
            component.state = Stopped
            logger.info(s"Starting $componentName instance $id")
          } else logger.warn(s"Zipkin $componentName instance $id already started")
          component
        }
      }

      if (timeout.toMillis > 0) {
        val ok = fetchCollection.forall(_.waitFor(Running, timeout))
        if (ok) response.getWriter.println(createApiResponse(true, s"Started $componentName instance(s) $idExpr", Some(components)))
        else response.getWriter.println(createApiResponse(false, s"Start $componentName instance(s) $idExpr timed out after $timeout", None))
      }
    })
  }

  private def handleStop[E <: ZipkinComponent](request: HttpServletRequest, response: HttpServletResponse,
                                               fetchCollection: => mutable.Buffer[E], componentName: String,
                                               createApiResponse: (Boolean, String, Option[List[E]]) => JsValue) {
    processExistingInstances(request, response, fetchCollection, componentName, createApiResponse, { (ids, idExpr) =>
      val components = ids.flatMap { id =>
        fetchCollection.find(_.id == id).map(Scheduler.stopInstance)
      }
      Scheduler.cluster.save()
      response.getWriter.println(createApiResponse(true, s"Stopped $componentName instance(s) $idExpr", Some(components)))
    })
  }

  private def handleRemove[E <: ZipkinComponent](request: HttpServletRequest, response: HttpServletResponse,
                                                 fetchCollection: => mutable.Buffer[E], componentName: String,
                                                 createApiResponse: (Boolean, String, Option[List[E]]) => JsValue) {
    processExistingInstances(request, response, fetchCollection, componentName, createApiResponse, { (ids, idExpr) =>
      val components = ids.flatMap { id =>
        fetchCollection.find(_.id == id).map { zc =>
          fetchCollection -= zc
          Scheduler.stopInstance(zc)
        }
      }
      Scheduler.cluster.save()
      response.getWriter.println(createApiResponse(true, s"Removed $componentName instance(s) $idExpr", Some(components)))
    })
  }

  private def handleConfig[E <: ZipkinComponent](request: HttpServletRequest, response: HttpServletResponse,
                                                 fetchCollection: => mutable.Buffer[E], componentName: String,
                                                 createApiResponse: (Boolean, String, Option[List[E]]) => JsValue) {
    processExistingInstances(request, response, fetchCollection, componentName, createApiResponse, { (ids, idExpr) =>
      val components = ids.flatMap { id =>
        fetchCollection.find(_.id == id).map { component =>
          request.getParameterMap.toMap.foreach {
            //TODO: parsing error handling
            case ("constraints", values) => component.constraints ++= Try(Constraint.parse(values.head)).getOrElse(Map())
            case ("port", Array(ports)) => component.config.ports = URange.parseRanges(ports)
            case ("adminPort", Array(ports)) => component.config.adminPorts = URange.parseRanges(ports)
            case ("cpu", values) => component.config.cpus = Try(values.head.toDouble).getOrElse(component.config.cpus)
            case ("mem", values) => component.config.mem = Try(values.head.toDouble).getOrElse(component.config.mem)
            case ("flags", values) => component.config.flags = Try(Util.parseMap(values.head)).getOrElse(component.config.flags)
            case ("env", values) => component.config.env = Try(Util.parseMap(values.head)).getOrElse(component.config.env)
            case ("configFile", values) => component.config.configFile = Some(values.head)
            case other => logger.debug(s"Got invalid configuration value: $other")
          }
          component
        }
      }

      Scheduler.cluster.save()
      response.getWriter.println(createApiResponse(true, s"Updated configuration for Zipkin $componentName instance(s) $idExpr", Some(components)))
    })
  }

  private def processExistingInstances[E <: ZipkinComponent](request: HttpServletRequest,
                                                             response: HttpServletResponse,
                                                             fetchCollection: => mutable.Buffer[E],
                                                             componentName: String,
                                                             createApiResponse: (Boolean, String, Option[List[E]]) => JsValue,
                                                             action: (List[String], String) => Unit) {
    val idExpr = request.getParameter("id")
    val ids = Util.expandIds(idExpr, fetchCollection)
    val missing = ids.filter(id => !fetchCollection.exists(id == _.id))
    if (missing.nonEmpty) response.getWriter.println(ApiResponse(success = false,
      s"Zipkin $componentName instance(s) ${missing.mkString(",")} do not exist", None))
    else action(ids, idExpr)
  }

  private def handleList[E <: ZipkinComponent](request: HttpServletRequest, response: HttpServletResponse,
                                               fetchCollection: => mutable.Buffer[E], componentName: String,
                                               createApiResponse: (Boolean, String, Option[List[E]]) => JsValue) {
    Option(request.getParameter("id")).map(Util.expandIds(_, fetchCollection)) match {
      case None => response.getWriter.println(createApiResponse(true, "", Some(fetchCollection.toList)))
      case Some(ids) =>
        ids.filter(id => !fetchCollection.exists(id == _.id)) match {
          case Nil => response.getWriter.println(createApiResponse(true, "",
            Some(fetchCollection.filter(zc => ids.contains(zc.id)).toList)))
          case nonExistentIds => response.getWriter.println(createApiResponse(false,
            s"Zipkin $componentName instance(s) ${nonExistentIds.mkString(",")} do not exist", None))
        }
    }
  }
}