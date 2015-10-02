package net.elodina.mesos.zipkin.utils

import java.net.{Inet4Address, InetAddress, NetworkInterface}
import java.util
import scala.collection.JavaConversions._

class BindAddress(s: String) {
  private var _source: String = null
  private var _value: String = null

  def source: String = _source

  def value: String = _value

  parse()

  def parse() = {
    val idx = s.indexOf(":")
    if (idx != -1) {
      _source = s.substring(0, idx)
      _value = s.substring(idx + 1)
    } else {
      _value = s
    }

    if (source != null && source != "if")
      throw new IllegalArgumentException(s)
  }

  def resolve(): String = {
    _source match {
      case null => resolveAddress(_value)
      case "if" => resolveInterfaceAddress(_value)
      case _ => throw new IllegalStateException("Failed to resolve " + s)
    }
  }

  def resolveAddress(addressOrMask: String): String = {
    if (!addressOrMask.endsWith("*")) return addressOrMask
    val prefix = addressOrMask.substring(0, addressOrMask.length - 1)

    for (ni <- NetworkInterface.getNetworkInterfaces) {
      val address = ni.getInetAddresses.find(_.getHostAddress.startsWith(prefix)).orNull
      if (address != null) return address.getHostAddress
    }

    throw new IllegalStateException("Failed to resolve " + s)
  }

  def resolveInterfaceAddress(name: String): String = {
    val ni = NetworkInterface.getNetworkInterfaces.find(_.getName == name).orNull
    if (ni == null) throw new IllegalStateException("Failed to resolve " + s)

    val addresses: util.Enumeration[InetAddress] = ni.getInetAddresses
    val address = addresses.find(_.isInstanceOf[Inet4Address]).orNull
    if (address != null) return address.getHostAddress

    throw new IllegalStateException("Failed to resolve " + s)
  }

  override def hashCode(): Int = 31 * _source.hashCode + _value.hashCode

  override def equals(o: scala.Any): Boolean = {
    if (o == null || o.getClass != this.getClass) return false
    val address = o.asInstanceOf[BindAddress]
    _source == address._source && _value == address._value
  }

  override def toString: String = s
}
