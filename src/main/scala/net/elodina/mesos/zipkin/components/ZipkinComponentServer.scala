package net.elodina.mesos.zipkin.components

import java.io.File

import net.elodina.mesos.zipkin.http.HttpServer

import scala.sys.process.Process
import scala.sys.process.ProcessBuilder

class ZipkinComponentServer {

  var process: Process = null

  def isStarted = Option(process).isDefined

  def start(taskConfig: TaskConfig, taskId: String) = {
    val jarMask = ZipkinComponent.getComponentFromTaskId(taskId) match {
      case "collector" => HttpServer.collectorMask
      case "query" => HttpServer.queryMask
      case "web" => HttpServer.webMask
      case _ => throw new IllegalArgumentException(s"Illegal component name found in task id: $taskId")
    }
    val distToLaunch = initJar(jarMask)
    process = configureProcess(taskConfig, distToLaunch).run()
    //TODO: consider logs redirect
  }

  def await(): Option[Int] = {
    if (isStarted) Some(process.exitValue()) else None
  }

  def stop() {
    if (isStarted) process.destroy()
  }

  private def initJar(jarMask: String): File = {
    new File(".").listFiles().find(file => file.getName.matches(jarMask)) match {
      case None => throw new IllegalStateException("Corresponding jar not found")
      case Some(componentDist) => componentDist
    }
  }

  private def configureProcess(taskConfig: TaskConfig, distToLaunch: File): ProcessBuilder = {
    val configFileArg = taskConfig.configFile.map(Seq("-f", _))
    var command = Seq("java", "-jar", distToLaunch.getCanonicalPath)
    configFileArg.foreach(command ++= _)
    command ++= taskConfig.flags.map { case (k: String, v: String) => s"--$k=$v" }
    Process(command, Some(new File(".")), taskConfig.envVariables.toList:_*)
  }
}
