package lv.uniso.migration

import java.sql.Connection

import scala.collection.mutable

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

  def get_int(conn: Connection, sql: String): Int = {
    val ps = conn.createStatement()
    // Printer.msg(s"$sql")

    val rs = ps.executeQuery(sql)
    rs.next
    val ret = rs.getInt(1)
    rs.close()
    ps.close()
    ret
  }

  def get_int2(conn: Connection, sql: String): (Int, Int) = {
    val ps = conn.createStatement()
    // Printer.msg(s"$sql")

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
