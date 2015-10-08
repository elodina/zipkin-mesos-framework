package net.elodina.mesos.zipkin

package object zipkin {

  sealed trait State

  case object Added extends State

  case object Stopped extends State

  case object Staging extends State

  case object Running extends State

  case object Reconciling extends State
}
