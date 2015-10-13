package net.elodina.mesos.zipkin.cli

import java.io.PrintStream

import joptsimple.OptionParser
import net.elodina.mesos.zipkin.components._
import net.elodina.mesos.zipkin.http.ApiResponse
import net.elodina.mesos.zipkin.components.ZipkinComponent._
import net.elodina.mesos.zipkin.utils.Util
import play.api.libs.json.JsValue

object ZipkinComponentCli {

  def handle(cmd: String, subCmd: Option[String] = None, cmdArgs: Array[String] = Array(), help: Boolean = false) {
    val jsonDeserializer = cmd match {
      case "collector" => (x: JsValue) => x.as[ApiResponse[Collector]]
      case "query" => (x: JsValue) => x.as[ApiResponse[QueryService]]
      case "web" => (x: JsValue) => x.as[ApiResponse[WebService]]
    }

    if (help || subCmd.isEmpty) {
      handleHelp(cmd)
      return
    }

    def containsIdExpr = cmdArgs.length > 0 && !cmdArgs(0).startsWith("-")

    if (!containsIdExpr && subCmd.get != "list") {
      handleHelp(cmd)
      printLine()
      throw new CliError("argument required")
    }

    val idExpr = {
      cmdArgs(0)
    }
    val refinedArgs = {
      cmdArgs.slice(1, cmdArgs.length)
    }

    subCmd.get match {
      case "list" => handleList(cmd, jsonDeserializer, Some(idExpr))
      case "add" | "config" => handleAddConfig(cmd, jsonDeserializer, Some(idExpr), help = false,
        add = subCmd.get == "add", refinedArgs)
      case "remove" => handleRemove(cmd, jsonDeserializer, Some(idExpr))
      case "start" => handleStart(cmd, jsonDeserializer, Some(idExpr), help = false, refinedArgs)
      case "stop" => handleStop(cmd, jsonDeserializer, Some(idExpr))
      case _ => throw new CliError(s"unsupported Zipkin $cmd command subCmd.get")
    }
  }

  def handleHelp(cmd: String, subCmd: Option[String] = None): Unit = {
    subCmd match {
      case None =>
        printLine(s"Zipkin $cmd management commands\nUsage: $cmd <command>\n")
        printCmds()

        printLine()
        printLine(s"Run `help $cmd <command>` to see details of specific command")
      case Some("list") =>
        handleList(cmd, help = true)
      case Some("add") | Some("config") =>
        handleAddConfig(cmd, help = true)
      case Some("remove") =>
        handleRemove(cmd, help = true)
      case Some("start") =>
        handleStart(cmd, help = true)
      case Some("stop") =>
        handleStop(cmd, help = true)
      case _ =>
        throw new CliError(s"unsupported command ${subCmd.get}")
    }
  }

  private def handleAddConfig[E <: ZipkinComponent](componentName: String, deserializeJson: JsValue => ApiResponse[E] = ApiResponse(),
                                            expr: Option[String] = None, help: Boolean = false, add: Boolean = true,
                                            args: Array[String] = Array()): Unit = {
    val parser = newParser()
    configureTypedCLParser[java.lang.Double](parser, Map(
    "cpu" -> "cpu amount (0.5, 1, 2)",
    "mem" -> "mem amount in Mb"
    ))
    configureCLParser(parser, Map(
      "flags" -> "App flags",
      "envVariables" -> "Environment variables",
      "ports" -> "port or range (31092, 31090..31100). Default - auto",
      "configFile" -> "Configuration file to launch an instance"
    ))

    val cmd = if (add) "add" else "config"
    if (help) {
      printHelp(componentName, cmd, Some(parser), printConstraints = true)

      return
    }

    val options = parseOptions(parser, args)

    val params = collection.mutable.Map[String, String]("id" -> expr.getOrElse(throw new IllegalArgumentException("id expr not specified")))
    readCLProperty[java.lang.Double]("cpu", options).foreach(x => params += ("cpu" -> x.toString))
    readCLProperty[java.lang.Double]("mem", options).foreach(x => params += ("mem" -> x.toString))
    readCLProperty[String]("flags", options).foreach(x => params += ("flags" -> x))
    readCLProperty[String]("envVariables", options).foreach(x => params += ("envVariables" -> x))
    readCLProperty[String]("ports", options).foreach(x => params += ("ports" -> x))
    readCLProperty[String]("configFile", options).foreach(x => params += ("configFile" -> x))

    val response = deserializeJson(sendRequest(s"/$componentName/$cmd", params.toMap))

    processApiResponse(response)
  }

  private def handleRemove[E <: ZipkinComponent](componentName: String, deserializeJson: JsValue => ApiResponse[E] = ApiResponse(),
                                         expr: Option[String] = None, help: Boolean = false): Unit = {
    if (help) {
      printHelp(componentName, "remove")
      return
    }

    val response = deserializeJson(sendRequest(s"/$componentName/remove",
      expr.map(ids => Map("id" -> ids)).getOrElse(throw new IllegalArgumentException("id expr not specified"))))
    processApiResponse(response)
  }

  private def handleStop[E <: ZipkinComponent](componentName: String, deserializeJson: JsValue => ApiResponse[E] = ApiResponse(),
                                       expr: Option[String] = None, help: Boolean = false): Unit = {
    if (help) {
      printHelp(componentName, "stop")
      return
    }
    val response = deserializeJson(sendRequest(s"/$componentName/stop",
      expr.map(ids => Map("id" -> ids)).getOrElse(throw new IllegalArgumentException("id expr not specified"))))
    processApiResponse(response)
  }

  private def handleStart[E <: ZipkinComponent](componentName: String, deserializeJson: JsValue => ApiResponse[E] = ApiResponse(),
                                        expr: Option[String] = None, help: Boolean = false, args: Array[String] = Array()): Unit = {
    val parser = newParser()

    configureCLParser(parser, Map("timeout" -> ("Time to wait for server to be started. " +
      "Should be a parsable Scala Duration value. Defaults to 60s. Optional")))

    if (help) {
      printHelp(componentName, "start", Some(parser))
      return
    }

    val params = collection.mutable.Map[String, String]("id" -> expr.getOrElse(throw new IllegalArgumentException("id expr not specified")))
    readCLProperty[String]("timeout", parseOptions(parser, args)).foreach(x => params += ("timeout" -> x.toString))

    val response = deserializeJson(sendRequest(s"/$componentName/start", params.toMap))
    processApiResponse(response)
  }

  private def handleList[E <: ZipkinComponent](componentName: String, deserializeJson: JsValue => ApiResponse[E] = ApiResponse(),
                                       expr: Option[String] = None, help: Boolean = false): Unit = {
    if (help) {
      printHelp(componentName, "list")
      return
    }

    val response = deserializeJson(sendRequest(s"/$componentName/list", expr.map(ids => Map("id" -> ids)).getOrElse(Map())))
    response.value.foreach(_.foreach(printZipkinComponent(_)))
  }

  private def printCmds(): Unit = {
    printLine("Commands:")
    printLine("list       - list instances", 1)
    printLine("add        - add instances", 1)
    printLine("config     - update instances", 1)
    printLine("remove     - remove instances", 1)
    printLine("start      - start instances", 1)
    printLine("stop       - stop instances", 1)
  }

  private def printZipkinComponent[E <: ZipkinComponent](component: E, indent: Int = 0) = {
    printLine("instance:", indent)
    printLine(s"id: ${component.id}", indent + 1)
    printLine(s"state: ${component.state}", indent + 1)
    if (component.constraints.nonEmpty)
      printLine(s"constraints: ${Util.formatConstraints(component.constraints)}", indent + 1)
    printTaskConfig(component.config, indent + 1)
    printLine()
  }

  private def printTaskConfig(config: TaskConfig, indent: Int = 0) {
    printLine("config:", indent)
    printLine(s"cpu: ${config.cpus}", indent)
    printLine(s"mem: ${config.mem}", indent)
    val ports = config.ports match {
      case Nil => "auto"
      case _ => config.ports.mkString(",")
    }
    printLine(s"port: $ports", indent)
    printLine(s"envVariables: ${Util.formatMap(config.envVariables)}")
    printLine(s"flags: ${Util.formatMap(config.flags)}")
    config.configFile.foreach(cf => printLine(s"configFile: $cf"))
  }

  private def printIdExprExamples(out: PrintStream): Unit = {
    out.println("id-expr examples:")
    out.println("  0      - instance 0")
    out.println("  0,1    - instance 0,1")
    out.println("  0..2   - instance 0,1,2")
    out.println("  0,1..2 - instance 0,1,2")
    out.println("  *      - any instance")
    out.println("attribute filtering:")
    out.println("  *[rack=r1]           - any instance having rack=r1")
    out.println("  *[hostname=slave*]   - any instance on host with name starting with 'slave'")
    out.println("  0..4[rack=r1,dc=dc1] - any instance having rack=r1 and dc=dc1")
  }

  private def printConstraintExamples() {
    printLine("constraint examples:")
    printLine("like:slave0    - value equals 'slave0'", 1)
    printLine("unlike:slave0  - value is not equal to 'slave0'", 1)
    printLine("like:slave.*   - value starts with 'slave'", 1)
    printLine("unique         - all values are unique", 1)
    printLine("cluster        - all values are the same", 1)
    printLine("cluster:slave0 - value equals 'slave0'", 1)
    printLine("groupBy        - all values are the same", 1)
    printLine("groupBy:3      - all values are within 3 different groups", 1)
  }

  private def printHelp(componentName: String, cmd: String, parser: Option[OptionParser] = None,
                        printConstraints: Boolean = false): Unit = {
    printLine(s"${cmd.capitalize}\nUsage: $componentName $cmd [<$componentName-expr>] [options]\n")
    parser.foreach { p => p.printHelpOn(out); printLine() }

    handleGenericOptions(null, help = true)

    printLine()
    printIdExprExamples(out)
    if (printConstraints) {
      printLine()
      printConstraintExamples()
    }
  }

  private def processApiResponse[E <: ZipkinComponent](response: ApiResponse[E]): Unit = {
    if (!response.success) throw CliError(response.message)

    printLine(response.message)
    printLine()
    response.value.foreach(_.foreach(printZipkinComponent(_)))
  }
}
