package net.elodina.mesos.zipkin.mesos

import _root_.net.elodina.mesos.zipkin.utils.Str
import org.apache.log4j._
import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}
import org.apache.mesos.Protos._

object Executor extends org.apache.mesos.Executor {

  private val logger = Logger.getLogger(Executor.getClass)

  def main(args: Array[String]) {
    initLogging()

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    sys.exit(status)
  }

  override def shutdown(driver: ExecutorDriver): Unit = {
    logger.info("[shutdown]")
    //stopExecutor()
  }

  override def disconnected(driver: ExecutorDriver): Unit = {
    logger.info("[disconnected]")
  }

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {
    logger.info("[killTask] " + taskId.getValue)
    //stopExecutor()
  }

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    logger.info("[reregistered] " + Str.slave(slaveInfo))
  }

  override def error(driver: ExecutorDriver, message: String): Unit = {
    logger.info("[error] " + message)
  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {
    logger.info("[frameworkMessage] " + new String(data))
  }

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {
    logger.info("[registered] framework:" + Str.framework(frameworkInfo) + " slave:" + Str.slave(slaveInfo))
  }

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    logger.info("[launchTask] " + Str.task(task))

    // actually launch the task
  }

  private def initLogging() {
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    val logger = Logger.getLogger(Executor.getClass.getPackage.getName)
    logger.setLevel(if (System.getProperty("debug") != null) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")
    root.addAppender(new ConsoleAppender(layout))
  }
}
