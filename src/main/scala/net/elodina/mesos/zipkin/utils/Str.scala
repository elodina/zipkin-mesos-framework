package net.elodina.mesos.zipkin.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.mesos.Protos
import org.apache.mesos.Protos._
import collection.JavaConverters._
import scala.collection.JavaConversions._

object Str {
  def dateTime(date: Date): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX").format(date)
  }

  def framework(framework: FrameworkInfo): String = {
    var s = ""

    s += id(framework.getId.getValue)
    s += " name: " + framework.getName
    s += " hostname: " + framework.getHostname
    s += " failover_timeout: " + framework.getFailoverTimeout

    s
  }

  def master(master: MasterInfo): String = {
    var s = ""

    s += id(master.getId)
    s += " pid:" + master.getPid
    s += " hostname:" + master.getHostname

    s
  }

  def slave(slave: SlaveInfo): String = {
    var s = ""

    s += id(slave.getId.getValue)
    s += " hostname:" + slave.getHostname
    s += " port:" + slave.getPort
    s += " " + resources(slave.getResourcesList.asScala.toList)

    s
  }

  def offer(offer: Offer): String = {
    var s = ""

    s += offer.getHostname + id(offer.getId.getValue)
    s += " " + resources(offer.getResourcesList.asScala.toList)
    if (offer.getAttributesCount > 0) s += " " + attributes(offer.getAttributesList.asScala.toList)

    s
  }

  def offers(offers: Iterable[Offer]): String = {
    var s = ""

    for (offer <- offers)
      s += (if (s.isEmpty) "" else "\n") + Str.offer(offer)

    s
  }

  def task(task: TaskInfo): String = {
    var s = ""

    s += task.getTaskId.getValue
    s += " slave:" + id(task.getSlaveId.getValue)

    s += " " + resources(task.getResourcesList.asScala.toList)
    s += " data:" + new String(task.getData.toByteArray)

    s
  }

  def resources(resources: List[Protos.Resource]): String = {
    var s = ""

    val order: List[String] = "cpus mem disk ports".split(" ").toList
    for (resource <- resources.sortBy(r => order.indexOf(r.getName))) {
      if (!s.isEmpty) s += " "

      s += resource.getName
      if (resource.getRole != "*") s += "(" + resource.getRole + ")"
      s += ":"

      if (resource.hasScalar)
        s += "%.2f".format(resource.getScalar.getValue)

      if (resource.hasRanges) {
        for (range: org.apache.mesos.Protos.Value.Range <- resource.getRanges.getRangeList)
          s += "[" + range.getBegin + ".." + range.getEnd + "]"
      }
    }
    s
  }

  def attributes(attributes: List[Protos.Attribute]): String = {
    var s = ""

    for (attr <- attributes) {
      if (!s.isEmpty) s += ";"
      s += attr.getName + ":"

      if (attr.hasText) s += attr.getText.getValue
      if (attr.hasScalar) s +=  "%.2f".format(attr.getScalar.getValue)
    }

    s
  }

  def taskStatus(status: TaskStatus): String = {
    var s = ""
    s += status.getTaskId.getValue
    s += " " + status.getState.name()

    s += " slave:" + id(status.getSlaveId.getValue)

    if (status.getState != TaskState.TASK_RUNNING)
      s += " reason:" + status.getReason.name()

    if (status.getMessage != null && status.getMessage != "")
      s += " message:" + status.getMessage

    if (status.getData.size > 0)
      s += " data: " + status.getData.toStringUtf8

    s
  }

  def id(id: String): String = "#" + suffix(id, 5)

  def suffix(s: String, maxLen: Int): String = {
    if (s.length <= maxLen) return s
    s.substring(s.length - maxLen)
  }
}