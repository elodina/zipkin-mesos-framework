package net.elodina.mesos.zipkin

import java.io._
import java.net.{URL, HttpURLConnection, URLEncoder}
import java.util
import java.util.Properties
import net.elodina.mesos.zipkin.utils.Util
import play.api.libs.json.{Json, JsValue}

import scala.collection.JavaConversions._

import joptsimple.{BuiltinHelpFormatter, OptionParser, OptionException, OptionSet}

import scala.io.Source

package object cli {

  var api: String = null
  var out: PrintStream = System.out
  var err: PrintStream = System.err

  case class CliError(message: String) extends java.lang.Error(message) {}

  private[cli] def printLine(s: Object = "", indent: Int = 0): Unit = out.println("  " * indent + s)

  private[cli] def configureCLParser(optionParser: OptionParser, optionsMap: Map[String, String]) = {
    configureTypedCLParser[String](optionParser, optionsMap)
  }

  private[cli] def configureTypedCLParser[E](optionParser: OptionParser, optionsMap: Map[String, String]) = {
    optionsMap.foreach { it =>
      optionParser.accepts(it._1, it._2).withRequiredArg().ofType(classOf[E])
    }
  }

  private[cli] def parseOptions(parser: OptionParser, args: Array[String]): OptionSet = {
    try { parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new CliError(e.getMessage)
    }
  }

  private[cli] def readCLProperty[E](propName: String, options: OptionSet) = {Option(options.valueOf(propName).asInstanceOf[E])}

  private[cli] def handleGenericOptions(args: Array[String], help: Boolean = false): Array[String] = {
    val parser = newParser()
    parser.accepts("api", "Api url. Example: http://master:7000").withRequiredArg().ofType(classOf[java.lang.String])
    parser.allowsUnrecognizedOptions()

    if (help) {
      printLine("Generic Options")
      parser.printHelpOn(out)
      return args
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new CliError(e.getMessage)
    }

    resolveApi(options.valueOf("api").asInstanceOf[String])
    options.nonOptionArguments().toArray(new Array[String](0))
  }

  private[cli] def optionsOrFile(value: String): String = {
    if (!value.startsWith("file:")) return value

    val file = new File(value.substring("file:".length))
    if (!file.exists()) throw new CliError(s"File $file does not exists")

    val props: Properties = new Properties()
    val reader = new FileReader(file)
    try { props.load(reader) }
    finally { reader.close() }

    val map = new util.HashMap[String, String](props.toMap)
    Util.formatMap(map)
  }

  private[cli] def newParser(): OptionParser = {
    val parser: OptionParser = new OptionParser()
    parser.formatHelpWith(new BuiltinHelpFormatter(Util.terminalWidth, 2))
    parser
  }

  private[cli] def resolveApi(apiOption: String): Unit = {
    if (api != null) return

    if (apiOption != null) {
      api = apiOption
      return
    }

    if (System.getenv("ZM_API") != null) {
      api = System.getenv("ZM_API")
      return
    }

    if (Config.DEFAULT_FILE.exists()) {
      val props: Properties = new Properties()
      val stream: FileInputStream = new FileInputStream(Config.DEFAULT_FILE)
      props.load(stream)
      stream.close()

      api = props.getProperty("api")
      if (api != null) return
    }

    throw new CliError("Undefined api. Provide either cli option or config default value")
  }

  private[cli] def sendRequest(uri: String, params: Map[String, String]): JsValue = {
    def queryString(params: Map[String, String]): String = {
      var s = ""
      params.foreach { case (name, value) =>
        if (!s.isEmpty) s += "&"
        s += URLEncoder.encode(name, "utf-8")
        if (value != null) s += "=" + URLEncoder.encode(value, "utf-8")
      }
      s
    }

    val qs: String = queryString(params)
    val url: String = Config.getApi + (if (Config.getApi.endsWith("/")) "" else "/") + "api" + uri + "?" + qs

    val connection: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    var response: String = null
    try {
      try {
        response = Source.fromInputStream(connection.getInputStream).getLines().mkString
      }
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    Json.parse(response)
  }
}
