package net.elodina.mesos.zipkin.http

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import org.apache.log4j.Logger

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
  }
}