package lv.addresses.migration

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

object Util {

  val tz: ZoneId = ZoneId.of("Europe/Riga")


  // iekšā: yyyy-mm-dd hh:mm:ss, europe/riga formātā
  // ārā: proper java.sql.timestamps (essentially unixtime)
  def riga2utc(ymdhs: String): Timestamp = {
    if (ymdhs == null || ymdhs == "") {
      null
    } else {
      // yyyy-mm-dd HH:mm:ss
      // 2006-01-02 15:04:05
      // 0123456789012345678

      val y = ymdhs.slice(0, 4).toInt
      val m = ymdhs.slice(5, 7).toInt
      val d = ymdhs.slice(8, 10).toInt
      val hr = ymdhs.slice(11, 13).toInt
      val min = ymdhs.slice(14, 16).toInt
      val sec = ymdhs.slice(17, 19).toInt
      // println(s"$y $m $d $hr $min $sec")
      val zdt = ZonedDateTime.of(y, m, d, hr, min, sec, 0, tz)
      java.sql.Timestamp.from(zdt.toInstant)

    }
  }


}
