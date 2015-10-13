/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.elodina.mesos.zipkin.cli

object MainCli {

  def exec(args: Array[String]): Unit = {

    if (args.length == 0) {
      handleHelp()
      printLine()
      throw new CliError("command required")
    }

    val cmd = args(0)
    var cmdArgs = args.slice(1, args.length)

    if (cmd == "help") {
      handleHelp(if (args.length > 0) Some(args(0)) else None, if (args.length > 1) Some(args(1)) else None); return
    }

    if (cmd == "scheduler" && SchedulerCli.isEnabled) {
      SchedulerCli.handle(cmdArgs)
      return
    }

    cmdArgs = handleGenericOptions(cmdArgs)

    // rest of cmds require <subCmd>
    if (cmdArgs.length < 1) {
      handleHelp(Some(cmd)); printLine()
      throw new CliError("command required")
    }

    val subCmd = cmdArgs(0)
    val subCmdArgs = cmdArgs.slice(1, args.length)

    cmd match {
      case "collector" | "query" | "web" => ZipkinComponentCli.handle(cmd, Some(subCmd), subCmdArgs)
      case _ => throw new CliError("unsupported command " + cmd)
    }
  }

  private def handleHelp(cmd: Option[String] = None, subCmd: Option[String] = None): Unit = {
    cmd match {
      case None =>
        printLine("Usage: <command>\n")
        printCmds()

        printLine()
        printLine("Run `help <command>` to see details of specific command")
      case Some("help") =>
        printLine("Print general or command-specific help\nUsage: help [cmd [cmd]]")
      case Some("scheduler") =>
        if (!SchedulerCli.isEnabled) throw new CliError(s"unsupported command $cmd")
        SchedulerCli.handle(null, help = true)
      case Some("collector") | Some("query") | Some("web") =>
        ZipkinComponentCli.handle(cmd.get, subCmd, null, help = true)
      case _ =>
        throw new CliError(s"unsupported command $cmd")
    }
  }

  private def printCmds(): Unit = {
    printLine("Commands:")
    printLine("help [cmd [cmd]] - print general or command-specific help", 1)
    if (SchedulerCli.isEnabled) printLine("scheduler        - start scheduler", 1)
    printLine("collector        - Zipkin collector management commands", 1)
    printLine("query            - Zipkin query management commands", 1)
    printLine("web              - Zipkin wen management commands", 1)
  }
}
