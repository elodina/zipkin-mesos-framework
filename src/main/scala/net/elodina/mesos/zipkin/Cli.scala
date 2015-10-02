package net.elodina.mesos.zipkin

import net.elodina.mesos.zipkin.cli.MainCli

object Cli extends App {

  try { MainCli.exec(args) } catch {
    case e: Error =>
      System.err.println("Error: " + e.getMessage)
      System.exit(1)
  }
}
