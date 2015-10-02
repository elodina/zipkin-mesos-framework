package net.elodina.mesos.zipkin.http

import java.io.{PrintWriter, File}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import net.elodina.mesos.zipkin.Config
import org.apache.log4j.{Level, Logger}
import org.eclipse.jetty.server._
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}
import org.eclipse.jetty.util.thread.QueuedThreadPool

import scala.util.parsing.json.JSONObject

object HttpServer {
  var jar: File = null

  val logger = Logger.getLogger(HttpServer.getClass)
  var server: Server = null

  def start(resolveDeps: Boolean = true) {
    if (server != null) throw new IllegalStateException("started")
    // TODO: here we should check for whether dependencies are in place

    val threadPool = new QueuedThreadPool(Runtime.getRuntime.availableProcessors() * 16)
    threadPool.setName("Jetty")

    server = new Server(threadPool)
    val connector = new ServerConnector(server)
    connector.setPort(Config.apiPort)
    if (Config.bindAddress != null) connector.setHost(Config.bindAddress.resolve())
    connector.setIdleTimeout(60 * 1000)

    val handler = new ServletContextHandler
    handler.addServlet(new ServletHolder(new Servlet()), "/")
    handler.setErrorHandler(new ErrorHandler())

    server.setHandler(handler)
    server.addConnector(connector)
    server.start()

    if (Config.apiPort == 0) Config.replaceApiPort(connector.getLocalPort)
    logger.info("started on port " + connector.getLocalPort)
  }

  def stop() {
    if (server == null) throw new IllegalStateException("!started")

    server.stop()
    server.join()
    server = null

    logger.info("stopped")
  }

  def initLogging(): Unit = {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[JettyLog4jLogger].getName)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.WARN)
    Logger.getLogger("Jetty").setLevel(Level.WARN)
  }

  class ErrorHandler extends handler.ErrorHandler() {
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val code: Int = response.getStatus
      val error: String = response match {
        case response: Response => response.getReason
        case _ => ""
      }

      val writer: PrintWriter = response.getWriter

      if (request.getAttribute("jsonResponse") != null) {
        response.setContentType("application/json; charset=utf-8")
        writer.println("" + new JSONObject(Map("code" -> code, "error" -> error)))
      } else {
        response.setContentType("text/plain; charset=utf-8")
        writer.println(code + " - " + error)
      }

      writer.flush()
      baseRequest.setHandled(true)
    }
  }

  class JettyLog4jLogger extends org.eclipse.jetty.util.log.Logger {
    private var logger: Logger = Logger.getLogger("Jetty")

    def this(logger: Logger) {
      this()
      this.logger = logger
    }

    def isDebugEnabled: Boolean = logger.isDebugEnabled

    def setDebugEnabled(enabled: Boolean) = logger.setLevel(if (enabled) Level.DEBUG else Level.INFO)

    def getName: String = logger.getName

    def getLogger(name: String): org.eclipse.jetty.util.log.Logger = new JettyLog4jLogger(Logger.getLogger(name))

    def info(s: String, args: AnyRef*) = logger.info(format(s, args))

    def info(s: String, t: Throwable) = logger.info(s, t)

    def info(t: Throwable) = logger.info("", t)

    def debug(s: String, args: AnyRef*) = logger.debug(format(s, args))

    def debug(s: String, t: Throwable) = logger.debug(s, t)

    def debug(t: Throwable) = logger.debug("", t)

    def warn(s: String, args: AnyRef*) = logger.warn(format(s, args))

    def warn(s: String, t: Throwable) = logger.warn(s, t)

    def warn(s: String) = logger.warn(s)

    def warn(t: Throwable) = logger.warn("", t)

    def ignore(t: Throwable) = logger.info("Ignored", t)
  }

  private def format(s: String, args: AnyRef*): String = {
    var result: String = ""
    var i: Int = 0

    for (token <- s.split("\\{\\}")) {
      result += token
      if (args.length > i) result += args(i)
      i += 1
    }

    result
  }
}
