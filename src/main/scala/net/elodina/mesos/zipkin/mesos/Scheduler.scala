package net.elodina.mesos.zipkin.mesos


import java.util

import _root_.net.elodina.mesos.zipkin.Config
import _root_.net.elodina.mesos.zipkin.http.HttpServer
import com.google.protobuf.ByteString
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

object Scheduler extends org.apache.mesos.Scheduler {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var driver: SchedulerDriver = null

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = ???

  override def disconnected(driver: SchedulerDriver): Unit = ???

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = ???

  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = ???

  override def error(driver: SchedulerDriver, message: String): Unit = ???

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Unit = ???

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = ???

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = ???

  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = ???

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int): Unit = ???

  def start() {
    initLogging()
    logger.info(s"Starting ${getClass.getSimpleName}:\n$Config")

    //cluster.load()
    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(Config.user.getOrElse(""))
    //if (cluster.frameworkId != null) frameworkBuilder.setId(FrameworkID.newBuilder().setValue(cluster.frameworkId))
    frameworkBuilder.setRole(Config.frameworkRole)

    frameworkBuilder.setName(Config.frameworkName)
    frameworkBuilder.setFailoverTimeout(Config.frameworkTimeout.ms / 1000)
    frameworkBuilder.setCheckpoint(true)

    var credsBuilder: Credential.Builder = null
    Config.principal.foreach {
      principal =>
        frameworkBuilder.setPrincipal(principal)

        credsBuilder = Credential.newBuilder()
        credsBuilder.setPrincipal(principal)
        Config.secret.foreach { secret => credsBuilder.setSecret(ByteString.copyFromUtf8(secret)) }
    }

    val driver =
      if (credsBuilder != null) new MesosSchedulerDriver(Scheduler, frameworkBuilder.build, Config.getMaster, credsBuilder.build)
      else new MesosSchedulerDriver(Scheduler, frameworkBuilder.build, Config.getMaster)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() = HttpServer.stop()
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    System.exit(status)
  }

  private def initLogging() {
    HttpServer.initLogging()
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.I0Itec.zkclient").setLevel(Level.WARN)

    val logger = Logger.getLogger(Scheduler.getClass)
    logger.setLevel(if (Config.debug) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")

    val appender = Config.log match {
      case Some(log) => new DailyRollingFileAppender(layout, log.getPath, "'.'yyyy-MM-dd")
      case None => new ConsoleAppender(layout)
    }

    root.addAppender(appender)
  }
}
