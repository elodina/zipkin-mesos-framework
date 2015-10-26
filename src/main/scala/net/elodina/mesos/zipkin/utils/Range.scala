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

  def dropFirst: Option[Range] = {
    if (start != end) {
      Some(Range(start + 1, end))
    } else None
  }

  def dropLast: Option[Range] = {
    if (start != end) {
      Some(Range(start, end - 1))
    } else None
  }

  def removeValue(value: Int): List[Range] = {
    if (value == start) {
      dropFirst.map(List(_)).getOrElse(Nil)
    } else if (value > start && value < end) {
      val headRange = values.takeWhile(_ < value)
      val tailRange = values.dropWhile(_ <= value)
      List(
        Range(headRange.head, headRange.last),
        Range(tailRange.head, headRange.last)
      )
    } else if (value == end) {
      dropLast.map(List(_)).getOrElse(Nil)
    } else Nil
  }

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

  /**
   * Overlaps one list range against another, seeking single Int value.
   *
   * @param ranges first list range to overlap against
   * @param rangesToUpdate ranges to subtract found value from
   * @return found value and updated list range
   */
  def overlapAndUpdate(ranges: List[Range], rangesToUpdate: List[Range]): (Option[Int], List[Range]) = {
    var value: Option[Int] = None
    if (ranges == Nil) {
      var updRanges = rangesToUpdate
      rangesToUpdate.headOption.foreach { headRange =>
        value = Some(headRange.start)
        updRanges = headRange.dropFirst.map(_ :: rangesToUpdate.drop(1)).getOrElse(rangesToUpdate.drop(1))
      }
      (value, updRanges)
    } else {
      var rangesToAdd: List[Range] = Nil
      var updRanges = rangesToUpdate.takeWhile {
        range =>
          ranges.flatMap(range.overlap).headOption.foreach { headRange =>
            value = Some(headRange.start)
            rangesToAdd = range.removeValue(headRange.start)
          }
          value.isEmpty
      }
      val leftToAdd = rangesToUpdate.size - updRanges.size - 1
      updRanges ++= rangesToAdd
      if (leftToAdd > 0) {
        updRanges ++= rangesToUpdate.view.zipWithIndex.foldLeft(List[Range]()) {
          (list, elem) =>
            if (elem._2 >= rangesToUpdate.size - leftToAdd) {
              elem._1 :: list
            } else list
        }
      }
      (value, updRanges)
    }
  }
}
