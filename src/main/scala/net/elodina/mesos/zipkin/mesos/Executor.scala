package net.elodina.mesos.zipkin.mesos

import java.io.{PrintWriter, StringWriter}

import net.elodina.mesos.zipkin.components.{ZipkinComponentServer, TaskConfig}
import net.elodina.mesos.zipkin.utils.Str
import org.apache.log4j._
import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}
import org.apache.mesos.Protos._
import play.api.libs.json.Json

object Executor extends org.apache.mesos.Executor {

  private val logger = Logger.getLogger(Executor.getClass)

  private val zipkinServer = new ZipkinComponentServer

  def main(args: Array[String]) {
    initLogging()

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    sys.exit(status)
  }

  override def shutdown(driver: ExecutorDriver): Unit = {
    logger.info("[shutdown]")
    stopExecutor()
  }

  override def disconnected(driver: ExecutorDriver): Unit = {
    logger.info("[disconnected]")
  }

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {
    logger.info("[killTask] " + taskId.getValue)
    stopExecutor()
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

    new Thread {
      override def run() {
        setName("Zipkin")

        try {
          val taskConfig = Json.parse(task.getData.toStringUtf8).as[TaskConfig]
          zipkinServer.start(taskConfig, task.getTaskId.getValue)
          driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId).setState(TaskState.TASK_RUNNING).build)
          zipkinServer.await().foreach(exitCode => if (exitCode != 0) {
            logger.error(s"Zipkin component process finished with exitCode $exitCode")
          })
          // TODO: consider sending TASK_FAILED on non-zero exit code
          driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId).setState(TaskState.TASK_FINISHED).build)
        } catch {
          case t: Throwable =>
            logger.error("", t)
            sendTaskFailed(driver, task, t)
        } finally {
          stopExecutor()
        }
      }
    }.start()

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

  private def sendTaskFailed(driver: ExecutorDriver, task: TaskInfo, t: Throwable) {
    val stackTrace = new StringWriter()
    t.printStackTrace(new PrintWriter(stackTrace, true))

    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId).setState(TaskState.TASK_FAILED)
      .setMessage("" + stackTrace).build)
  }

  private[zipkin] def stopExecutor(async: Boolean = false) {
    def triggerStop() {
      if (zipkinServer.isStarted) zipkinServer.stop()
      //TODO stop driver here?
    }

    if (async) {
      new Thread() {
        override def run() {
          setName("ExecutorStopper")
          triggerStop()
        }
      }
    } else triggerStop()
  }
}
