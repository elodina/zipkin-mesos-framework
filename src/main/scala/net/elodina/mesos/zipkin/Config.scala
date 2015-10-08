/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.elodina.mesos.zipkin

import java.io.{File, FileInputStream}
import java.net.URI
import java.util.Properties

import net.elodina.mesos.zipkin.utils.{BindAddress, Period}

object Config {
  val DEFAULT_FILE = new File("zipkin-mesos.properties")

  var debug: Boolean = false
  var storage: String = "file:zipkin-mesos.json"

  var master: Option[String] = None
  var principal: Option[String] = None
  var secret: Option[String] = None
  var user: Option[String] = None

  var frameworkName: String = "zipkin"
  var frameworkRole: String = "*"
  var frameworkTimeout: Period = new Period("30d")

  var jre: Option[File] = None
  var log: Option[File] = None
  var api: Option[String] = None
  var bindAddress: Option[BindAddress] = None
  var zk: Option[String] = None

  def apiPort: Int = {
    val port = new URI(getApi).getPort
    if (port == -1) 80 else port
  }

  def replaceApiPort(port: Int): Unit = {
    val prev: URI = new URI(getApi)
    api = Some("" + new URI(
      prev.getScheme, prev.getUserInfo,
      prev.getHost, port,
      prev.getPath, prev.getQuery, prev.getFragment
    ))
  }

  def getApi: String = {
    api.getOrElse(throw new Error("api not initialized"))
  }

  def getMaster: String = {
    master.getOrElse(throw new Error("master not initialized"))
  }

  def getZk: String = {
    master.getOrElse(throw new Error("zookeeper not initialized"))
  }

  private[zipkin] def loadFromFile(file: File): Unit = {
    val props: Properties = new Properties()
    val stream: FileInputStream = new FileInputStream(file)

    props.load(stream)
    stream.close()

    if (props.containsKey("debug")) debug = java.lang.Boolean.valueOf(props.getProperty("debug"))
    if (props.containsKey("storage")) storage = props.getProperty("storage")

    if (props.containsKey("master")) master = Some(props.getProperty("master"))
    if (props.containsKey("user")) user = Some(props.getProperty("user"))
    if (props.containsKey("principal")) principal = Some(props.getProperty("principal"))
    if (props.containsKey("secret")) secret = Some(props.getProperty("secret"))

    if (props.containsKey("framework-name")) frameworkName = props.getProperty("framework-name")
    if (props.containsKey("framework-role")) frameworkRole = props.getProperty("framework-role")
    if (props.containsKey("framework-timeout")) frameworkTimeout = new Period(props.getProperty("framework-timeout"))

    if (props.containsKey("jre")) jre = Some(new File(props.getProperty("jre")))
    if (props.containsKey("log")) log = Some(new File(props.getProperty("log")))
    if (props.containsKey("api")) api = Some(props.getProperty("api"))
    if (props.containsKey("bind-address")) bindAddress = Some(new BindAddress(props.getProperty("bind-address")))
    if (props.containsKey("zk")) zk = Some(props.getProperty("zk"))
  }

  override def toString: String = {
    s"""
       |debug: $debug, storage: $storage
        |mesos: master=$master, user=${if (user.isEmpty || user.get.isEmpty) "<default>" else user}
        |principal=${principal.getOrElse("<none>")}, secret=${if (secret.isDefined) "*****" else "<none>"}
        |framework: name=$frameworkName, role=$frameworkRole, timeout=$frameworkTimeout
        |api: $api, bind-address: ${bindAddress.getOrElse("<all>")}, zk: $zk, jre: ${jre.getOrElse("<none>")}
    """.stripMargin.trim
  }
}
