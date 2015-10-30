package net.elodina.mesos.zipkin.cli

import java.io.File

import joptsimple.{NonOptionArgumentSpec, OptionParser, OptionException, OptionSet}
import net.elodina.mesos.zipkin.Config
import net.elodina.mesos.zipkin.mesos.Scheduler
import net.elodina.mesos.zipkin.utils.{BindAddress, Period}

object SchedulerCli {
  def isEnabled: Boolean = System.getenv("ZM_NO_SCHEDULER") == null

  def handle(args: Array[String], help: Boolean = false): Unit = {
    val parser = newParser()

    parser.accepts("debug", "Debug mode. Default - " + Config.debug)
      .withRequiredArg().ofType(classOf[java.lang.Boolean])

    parser.accepts("genTraces", "Make scheduler generate traces by sending random framework messages from executor to scheduler. Default - " + Config.genTraces)
      .withRequiredArg().ofType(classOf[java.lang.Boolean])

    configureCLParser(parser,
      Map(
        "storage" -> ("""Storage for cluster state. Examples:
                        | - file:zipkin-mesos.json
                        | - zk:/zipkin-mesos
                        |Default - """.stripMargin + Config.storage),
        "master" -> """Master connection settings. Examples:
                      | - master:5050
                      | - master:5050,master2:5050
                      | - zk://master:2181/mesos
                      | - zk://username:password@master:2181
                      | - zk://master:2181,master2:2181/mesos""".stripMargin,
        "user" -> "Mesos user to run tasks. Default - none",
        "principal" -> "Principal (username) used to register framework. Default - none",
        "secret" -> "Secret (password) used to register framework. Default - none",
        "framework-name" -> ("Framework name. Default - " + Config.frameworkName),
        "framework-role" -> ("Framework role. Default - " + Config.frameworkRole),
        "framework-timeout" -> ("Framework timeout (30s, 1m, 1h). Default - " + Config.frameworkTimeout),
        "api" -> "Api url. Example: http://master:7000",
        "bind-address" -> "Scheduler bind address (master, 0.0.0.0, 192.168.50.*, if:eth1). Default - all",
        "log" -> "Log file to use. Default - stdout."
      )
    )

    val configArg = parser.nonOptions()

    if (help) {
      printLine("Start scheduler \nUsage: scheduler [options] [config.properties]\n")
      parser.printHelpOn(out)
      return
    }

    var options: OptionSet = null
    try {
      options = parser.parse(args: _*)
    } catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new CliError(e.getMessage)
    }

    fetchConfigFile(options, configArg).foreach { configFile =>
      printLine("Loading config defaults from " + configFile)
      Config.loadFromFile(configFile)
    }

    loadConfigFromArgs(options)

    Scheduler.start()
  }

  private def fetchConfigFile(options: OptionSet, configArg: NonOptionArgumentSpec[String]): Option[File] = {
    Option(options.valueOf(configArg)) match {
      case Some(configArgValue) =>
        val configFile = new File(configArgValue)
        if (configFile.exists()) throw new CliError(s"config-file $configFile not found")
        Some(configFile)
      case None if Config.DEFAULT_FILE.exists() =>
        Some(Config.DEFAULT_FILE)
      case _ => None
    }
  }

  private def loadConfigFromArgs(options: OptionSet): Unit = {
    val provideOption = "Provide either cli option or config default value"

    readCLProperty[java.lang.Boolean]("debug", options).foreach(Config.debug = _)

    readCLProperty[java.lang.Boolean]("genTraces", options).foreach(Config.genTraces = _)

    readCLProperty[String]("storage", options).foreach(Config.storage = _)

    readCLProperty[String]("master", options).foreach(x => Config.master = Some(x))

    if (Config.master.isEmpty) throw new CliError(s"Undefined master. $provideOption")

    readCLProperty[String]("secret", options).foreach(x => Config.secret = Some(x))

    readCLProperty[String]("principal", options).foreach(x => Config.principal = Some(x))

    readCLProperty[String]("user", options).foreach(x => Config.user = Some(x))

    readCLProperty[String]("framework-name", options).foreach(Config.frameworkName = _)

    readCLProperty[String]("framework-role", options).foreach(Config.frameworkRole = _)

    readCLProperty[String]("framework-timeout", options).foreach {
      ft => try {
        Config.frameworkTimeout = new Period(ft)
      } catch {
        case e: IllegalArgumentException => throw new CliError("Invalid framework-timeout")
      }
    }

    readCLProperty[String]("api", options).foreach(x => Config.api = Some(x))

    if (Config.api.isEmpty) throw new CliError(s"Undefined api. $provideOption")

    readCLProperty[String]("bind-address", options).foreach {
      ba => try {
        Config.bindAddress = Some(new BindAddress(ba))
      } catch {
        case e: IllegalArgumentException => throw new CliError("Invalid bind-address")
      }
    }

    readCLProperty[String]("log", options).foreach(x => Config.log = Some(new File(x)))

    Config.log.foreach(log => printLine(s"Logging to $log"))
  }
}
