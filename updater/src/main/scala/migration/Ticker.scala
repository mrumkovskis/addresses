package lv.uniso.migration

class Ticker (text: String, count: Long, default_batchsize: Long) {

  var n = 0L
  var started = 0L
  var extra = ""

  var batchsize = default_batchsize

  def tick : Boolean = {
    n += 1
    if (n == 1) {
      // pirmā ieraksta laiku ignorē, tur var būt
      // select delajs liels iekšā
      started = System.currentTimeMillis / 1000
      false
    } else if (batchsize == 0) {
      val s_elapsed = System.currentTimeMillis / 1000 - started
      if (s_elapsed >= 20) {
        batchsize = 2 * good_round(n)
      }
      false

    } else if (n % batchsize == 0) {
      stats
      true
    } else {
      false
    }
  }

  def tick(extra_info: String) : Boolean = {
    extra = extra_info
    tick
  }

  def tick_force_stats = {
    n += 1
    stats
    true
  }

  def stats = {
      val s_elapsed = System.currentTimeMillis / 1000 - started
      val remaining = (count * s_elapsed / n) - s_elapsed
      val remaining_h = remaining / 3600
      val remaining_min = (remaining % 3600) / 60
      val remaining_s = remaining % 60
      var rem = if (remaining_h > 0) s"${remaining_h}h ${remaining_min}m" else s"${remaining_min}m ${remaining_s}s"
      val ex = if (extra == "") "" else s" ($extra)"
      if (n <= count) {
          Printer.msg(s"$text: $n / $count, $rem remaining$ex")
      } else {
          Printer.msg(s"$text: $n / $count, and still going$ex")
      }
  }

  def good_round (n:Long) : Long = {
    if (n < 10) n
    else if (n < 100) n - (n % 10)
    else if (n < 1000) n - (n % 100)
    else if (n < 10000) n - (n % 1000)
    else n - (n % 10000)
  }


  def startupBanner = {
    if (count == 0) {
      Printer.msg(s"$text: nothing to do")
    } else {
      Printer.msg(s"$text: ${count} records")
    }
  }

  startupBanner


}

