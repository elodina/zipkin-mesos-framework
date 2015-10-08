package net.elodina.mesos.zipkin.utils

case class Range(start: Int, end: Int) {
  def overlap(r: Range): Option[Range] = {
    var x: Range = this
    var y: Range = r
    if (x.start > y.start) {
      val t = x
      x = y
      y = t
    }
    assert(x.start <= y.start)

    if (y.start > x.end) return None
    assert(y.start <= x.end)

    val start = y.start
    val end = Math.min(x.end, y.end)
    Some(Range(start, end))
  }

  def values: List[Int] = (start to end).toList

  override def toString: String = if (start == end) s"$start" else s"$start..$end"
}

object Range {
  def apply(s: String): Range = parse(s)

  def apply(start: Int): Range = Range(start, start)

  private def parse(range: String): Range = {
    val idx = range.indexOf("..")

    if (idx == -1) {
      val value = range.toInt
      Range(value, value)
    } else {
      val start = range.substring(0, idx).toInt
      val end = range.substring(idx + 2).toInt
      if (start > end) throw new IllegalArgumentException("start > end")
      Range(start, end)
    }
  }

  def parseRanges(ranges: String): List[Range] = {
    if (ranges.isEmpty) Nil
    else ranges.split(",").map(parse).toList
  }
}
