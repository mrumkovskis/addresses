package lv.uniso.migration

import java.sql.Connection
import java.util.regex.Pattern

// simple adaptation of https://stackoverflow.com/questions/2309970/named-parameters-in-jdbc
class NamedStatement (conn: Connection, sql_template: String) {
  
  var fields:List[String] = List()
  val ps: java.sql.PreparedStatement = templatize()

  def templatize() = {


    var sql = sql_template
    val findParam= Pattern.compile("(?<!')(:[\\w]*)(?!')")
    val m = findParam.matcher(sql_template)
    while (m.find) {
      fields = fields :+ m.group().substring(1)
    }

    conn.prepareStatement(sql_template.replaceAll(findParam.pattern(), "?"))
  }

  def idx(field: String) = {
    val res = fields.indexOf(field) + 1
    if (res == 0) throw new Exception(s"Unable to find field ${field} in template")
    res

  }

  lazy val utc_calendar = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))

  def close = ps.close
  def executeUpdate = ps.executeUpdate
  def setBoolean(field: String, value: Boolean) = ps.setBoolean(idx(field), value)
  def setDecimal(field: String, value: java.math.BigDecimal) = ps.setBigDecimal(idx(field), value)
  def setString(field: String, value: String) = ps.setString(idx(field), value)
  def setDate(field: String, value: java.sql.Date) = ps.setDate(idx(field), value)
  def setInt(field: String, value: Int) = ps.setInt(idx(field), value)
  def setLong(field: String, value: Long) = ps.setLong(idx(field), value)
  def setLongNull(field: String) = ps.setNull(idx(field), java.sql.Types.INTEGER)
  def setTimestamp(field: String, value: java.sql.Timestamp) = ps.setTimestamp(idx(field), value, utc_calendar)

  def addBatch = ps.addBatch
  def runBatch : Unit = {
    ps.executeBatch
    ps.clearBatch
  }

}


