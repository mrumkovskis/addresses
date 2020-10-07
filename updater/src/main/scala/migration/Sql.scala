package lv.addresses.migration

import java.sql.Connection
import java.time.Instant

import java.sql.{Connection, PreparedStatement}
import java.util.Calendar
import java.util.regex.Pattern

import scala.collection.mutable

// simple adaptation of https://stackoverflow.com/questions/2309970/named-parameters-in-jdbc
class NamedStatement (conn: Connection, sql_template: String) {
  
  var fields:List[String] = List()
  val ps: java.sql.PreparedStatement = templatize()

  def templatize(): PreparedStatement = {


    var sql = sql_template
    val findParam= Pattern.compile("(?<!')(:[\\w]*)(?!')")
    val m = findParam.matcher(sql_template)
    while (m.find) {
      fields = fields :+ m.group().substring(1)
    }

    conn.prepareStatement(sql_template.replaceAll(findParam.pattern(), "?"))
  }

  def idx(field: String): Int = {
    val res = fields.indexOf(field) + 1
    if (res == 0) throw new Exception(s"Unable to find field ${field} in template")
    res

  }

  lazy val utc_calendar: Calendar = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))

  def close(): Unit = ps.close()
  def executeUpdate: Int = ps.executeUpdate()
  def setBoolean(field: String, value: Boolean): Unit = ps.setBoolean(idx(field), value)
  def setDecimal(field: String, value: java.math.BigDecimal): Unit = ps.setBigDecimal(idx(field), value)
  def setString(field: String, value: String): Unit = ps.setString(idx(field), value)
  def setDate(field: String, value: java.sql.Date): Unit = ps.setDate(idx(field), value)
  def setInt(field: String, value: Int): Unit = ps.setInt(idx(field), value)
  def setLong(field: String, value: Long): Unit = ps.setLong(idx(field), value)
  def setLongNull(field: String): Unit = ps.setNull(idx(field), java.sql.Types.INTEGER)
  def setTimestamp(field: String, value: java.sql.Timestamp): Unit = ps.setTimestamp(idx(field), value, utc_calendar)

  def addBatch(): Unit = ps.addBatch()
  def runBatch() : Unit = {
    ps.executeBatch()
    ps.clearBatch()
  }

}






object Sql {

  def get_string(conn: Connection, sql: String): String = {
    val ps = conn.createStatement()
    val rs = ps.executeQuery(sql)
    val ret = if (rs.next) {
      rs.getString(1)
    } else ""

    rs.close()
    ps.close()
    ret
  }

  def get_instant(conn: Connection, sql: String): Option[Instant] = {
    val ps = conn.createStatement()
    val rs = ps.executeQuery(sql)
    val ret = if (rs.next) {
      Some(rs.getTimestamp(1).toInstant())
    } else None

    rs.close()
    ps.close()
    ret
  }


  def get_int(conn: Connection, sql: String): Int = {
    val ps = conn.createStatement()

    val rs = ps.executeQuery(sql)
    rs.next
    val ret = rs.getInt(1)
    rs.close()
    ps.close()
    ret
  }

  def get_int2(conn: Connection, sql: String): (Int, Int) = {
    val ps = conn.createStatement()

    val rs = ps.executeQuery(sql)
    rs.next
    val r1 = rs.getInt(1)
    val r2 = rs.getInt(2)
    rs.close()
    ps.close()
    (r1,r2)
  }

  def run(conn: Connection, sql: String): Unit = {
    val ps = conn.createStatement()
    ps.executeUpdate(sql)
    ps.close()
  }

  def get_longset(conn: Connection, sql: String): mutable.Set[Long] = {
    val out = scala.collection.mutable.Set[Long]()

    val ps = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
    ps.setFetchSize(10000)
    val rs = ps.executeQuery(sql)
    while (rs.next) {
      out += rs.getLong(1)
    }
    rs.close()
    ps.close()
    out
  }


  var cm: Boolean = true

  def commit_mode(mode: Boolean) : Unit =
  {
    cm = mode
  }

  def commit_enable(): Unit = commit_mode(true)
  def commit_disable(): Unit = commit_mode(false)

  def commit(conn: Connection) : Unit =
  {
    if (cm) {
      conn.commit()
    }
  }


}
