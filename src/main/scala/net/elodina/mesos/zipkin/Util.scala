package net.elodina.mesos.zipkin

import java.io.{IOException, File}
import java.util
import scala.collection.JavaConversions._

object Util {

  var terminalWidth: Int = getTerminalWidth
  private def getTerminalWidth: Int = {
    val file: File = File.createTempFile("getTerminalWidth", null)
    file.delete()

    var width = 80
    try {
      new ProcessBuilder(List("bash", "-c", "tput cols"))
        .inheritIO().redirectOutput(file).start().waitFor()

      val source = scala.io.Source.fromFile(file)
      width = try Integer.valueOf(source.mkString.trim) finally source.close()
    } catch {
      case e: IOException => /* ignore */
      case e: NumberFormatException => /* ignore */
    }

    file.delete()
    width
  }

  def formatMap(map: util.Map[String, _ <: Any], entrySep: Char = ',', valueSep: Char = '='): String = {
    def escape(s: String): String = {
      var result = ""

      for (c <- s.toCharArray) {
        if (c == entrySep || c == valueSep || c == '\\') result += "\\"
        result += c
      }

      result
    }

    var s = ""
    for ((k, v) <- map) {
      if (!s.isEmpty) s += entrySep
      s += escape(k)
      if (v != null) s += valueSep + escape("" + v)
    }

    s
  }
}
