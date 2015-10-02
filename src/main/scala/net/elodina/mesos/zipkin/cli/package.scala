package net.elodina.mesos.zipkin

import java.io.{PrintStream, FileInputStream, FileReader, File}
import java.util
import java.util.Properties
import scala.collection.JavaConversions._

import joptsimple.{BuiltinHelpFormatter, OptionParser, OptionException, OptionSet}

package object cli {

  var api: String = null
  var out: PrintStream = System.out
  var err: PrintStream = System.err

  class Error(message: String) extends java.lang.Error(message) {}

  def printLine(s: Object = "", indent: Int = 0): Unit = out.println("  " * indent + s)

  private[zipkin] def handleGenericOptions(args: Array[String], help: Boolean = false): Array[String] = {
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
        throw new Error(e.getMessage)
    }

    resolveApi(options.valueOf("api").asInstanceOf[String])
    options.nonOptionArguments().toArray(new Array[String](0))
  }

  private[zipkin] def optionsOrFile(value: String): String = {
    if (!value.startsWith("file:")) return value

    val file = new File(value.substring("file:".length))
    if (!file.exists()) throw new Error(s"File $file does not exists")

    val props: Properties = new Properties()
    val reader = new FileReader(file)
    try { props.load(reader) }
    finally { reader.close() }

    val map = new util.HashMap[String, String](props.toMap)
    Util.formatMap(map)
  }

  private[zipkin] def newParser(): OptionParser = {
    val parser: OptionParser = new OptionParser()
    parser.formatHelpWith(new BuiltinHelpFormatter(Util.terminalWidth, 2))
    parser
  }

  private[zipkin] def resolveApi(apiOption: String): Unit = {
    if (api != null) return

    if (apiOption != null) {
      api = apiOption
      return
    }

    if (System.getenv("KM_API") != null) {
      api = System.getenv("KM_API")
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

    throw new Error("Undefined api. Provide either cli option or config default value")
  }
}
