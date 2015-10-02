package net.elodina.mesos.zipkin.cli

import java.io.File

import joptsimple.{NonOptionArgumentSpec, OptionParser, OptionException, OptionSet}
import net.elodina.mesos.zipkin.Config
import net.elodina.mesos.zipkin.mesos.Scheduler
import net.elodina.mesos.zipkin.utils.{BindAddress, Period}

object SchedulerCli {
  def isEnabled: Boolean = System.getenv("KM_NO_SCHEDULER") == null

  def handle(args: Array[String], help: Boolean = false): Unit = {
    val parser = newParser()

    parser.accepts("debug", "Debug mode. Default - " + Config.debug)
      .withRequiredArg().ofType(classOf[java.lang.Boolean])

    configureParser(parser,
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
        "zk" -> """Kafka zookeeper.connect. Examples:
                  | - master:2181
                  | - master:2181,master2:2181""".stripMargin,
        "jre" -> "JRE zip-file (jre-7-openjdk.zip). Default - none.",
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
        throw new Error(e.getMessage)
    }

    loadConfig(options, configArg)

    val provideOption = "Provide either cli option or config default value"

    readProperty[java.lang.Boolean]("debug", options, { _ => Config.debug = _ })

    readProperty[String]("storage", options, { _ => Config.storage = _ })

    readProperty[String]("master", options, { _ => Config.master = _ })

    if (Config.master == null) throw new Error(s"Undefined master. $provideOption")

    readProperty[String]("secret", options, { _ => Config.secret = _ })

    readProperty[String]("principal", options, { _ => Config.principal = _ })

    readProperty[String]("user", options, { _ => Config.user = _ })

    readProperty[String]("framework-name", options, { _ => Config.frameworkName = _ })

    readProperty[String]("framework-role", options, { _ => Config.frameworkRole = _ })

    readProperty[String]("framework-timeout", options, {
      ft => try {
        Config.frameworkTimeout = new Period(ft)
      } catch {
        case e: IllegalArgumentException => throw new Error("Invalid framework-timeout")
      }
    })

    readProperty[String]("api", options, { _ => Config.api = _ })

    if (Config.api == null) throw new Error(s"Undefined api. $provideOption")

    readProperty[String]("bind-address", options, {
      ba => try {
        Config.bindAddress = new BindAddress(ba)
      } catch {
        case e: IllegalArgumentException => throw new Error("Invalid bind-address")
      }
    })

    readProperty[String]("zk", options, { _ => Config.zk = _ })

    if (Config.zk == null) throw new Error(s"Undefined zk. $provideOption")

    readProperty("jre", options, { _ => Config.jre = _ })

    if (Config.jre != null && !Config.jre.exists()) throw new Error("JRE file doesn't exists")

    readProperty[String]("log", options, { log => Config.log = new File(log) })

    if (Config.log != null) printLine(s"Logging to ${Config.log}")

    Scheduler.start()
  }

  private def loadConfig(options: OptionSet, configArg: NonOptionArgumentSpec[String]): Unit = {
    for {
      configFile <- Option(options.valueOf(configArg)) match {
        case Some(configArgValue) =>
          val configFile = new File(configArgValue)
          if (configFile.exists()) throw new Error(s"config-file $configFile not found")
          Some(configFile)
        case None if Config.DEFAULT_FILE.exists() =>
          Some(Config.DEFAULT_FILE)
        case _ => None
      }
    } yield {
      printLine("Loading config defaults from " + configFile)
      Config.load(configFile)
    }
  }

  private def readProperty[E](propName: String, options: OptionSet, gotValue: E => Unit): Unit = {
    for {
      propValue <- Option(options.valueOf(propName).asInstanceOf[E])
    } yield {
      gotValue(propValue)
    }
  }

  private def configureParser(optionsParser: OptionParser, optionsMap: Map[String, String]): Unit = {
    optionsMap.map { it =>
      optionsParser.accepts(it._1, it._2).withRequiredArg().ofType(classOf[String])
    }
  }
}
