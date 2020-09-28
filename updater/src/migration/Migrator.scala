package lv.uniso.migration

import scala.collection.mutable.HashMap
import java.sql.Connection

class Migrator (remoteSource:String, localDest:String, fieldDef:List[Tuple3[String, String, String]]) {

  val batch_size = 50000
  val fetch_size = 5000

  type OptMap = Map[String, String]

  def source = remoteSource
  def target = localDest

  val orafmt = "YYYY-MM-DD HH24:MI:SS"

  lazy val pk_field = fieldDef.find(_._2 == "pk").get

  def full_sync_always = {
    fieldDef.find( s => (s._2 == "datetag")) == None
  }

  def datetag_local = {
    (fieldDef.find( s => (s._2 == "datetag")).get)._1
  }
  def datetag_remote = {
    (fieldDef.find( s => (s._2 == "datetag")).get)._3
  }

  def insertSql = {

    val fields = fieldDef.filter(fd => fd._2 != "pk")
    val fds = s"${pk_field._1}, sync_synced, " + fields.map(_._1).mkString(", ")
    val ins = ":sync_id, now(), "       + fields.map(fd => s":${fd._1}").mkString(", ")

    // s"insert into $localDest ($fds) values ($ins) on conflict do nothing"
    // oh come on, 9.5 with "on conflict" support was released in jan-2016
    s"insert into $localDest ($fds) values ($ins)"

  }

  def updateSql = {

    val fields = fieldDef.filter(fd => fd._2 != "pk")
    val upds = fields.map(fd => s"${fd._1}=:${fd._1}").mkString(", ")

    s"update $localDest set $upds where ${pk_field._1}=:sync_id"

  }

  def remoteFieldsNeeded = {
    fieldDef.map{
      case (local, t, fd) => 
        if (t == "datetag" || t == "datetime") {
          s"to_char($fd, '$orafmt') as $local"
        } else {
          s"$fd as $local"
        }
    }
  }

  def createTableSql = {
    val defs = fieldDef.map( fd => {

      val sqlType = fd._2 match {
        case "pk" => "bigint primary key"
        case "int" => "bigint"
        case "string" => "varchar"
        case "yn" => "boolean"
        case "01" => "boolean"
        case "boolean" => "boolean"
        case "decimal" => "decimal(10, 3)"
        case "date" => "date"
        case "vertiba" => "decimal(20, 6)"
        // case "decimal_20_6" => "decimal(20, 6)"
        case "decimal_20_8" => "decimal(20, 8)"
        case "decimal_22_0" => "decimal(22, 0)"
        case "datetime" => "timestamp with time zone"
        case "datetag" => "timestamp with time zone"
        case "legacy_datetag" => "varchar"
        case default => throw new Exception(s"Unsupported field type $default")
      }
      s"  ${fd._1} $sqlType"

    }) .mkString(",\n")

    s"create table if not exists $localDest ($defs, sync_synced timestamp with time zone)"
  }

  def rebuild(write: Connection, truncate: Boolean) : Unit = {
    {
      val ps = write.prepareStatement(createTableSql)
      ps.executeUpdate
      ps.close
      write.commit
    }
  /*
    if (truncate) {
      val ps = write.prepareStatement(s"truncate table $localDest cascade")
      ps.executeUpdate
      ps.close
      write.commit
    }
    */

  }

  def getWhere(vzd: Connection, local: Connection, opts: OptMap) = {
    val dt_max = Sql.get_string(local, s"select max(${datetag_local}) from $localDest").substring(0, 19)
    Printer.msg(s"Fetching new records since $dt_max")
    s"where ${datetag_remote} > to_date('$dt_max', '$orafmt')"
  }

  def migrate(read: Connection, write: Connection, opts: OptMap) : Unit = {

    val n_existing = Sql.get_int(write, s"select count(*) from $localDest")

    if (opts("verify") == "true") {
      migrateFullSlurpstein(read, write)
    } else if (n_existing == 0) {
      // pārdzenam 1:1 optimizētā režīmā (straight_inserts = true)
      Printer.msg(s"Destination empty, running fast copy")
      migrateClassic(read, write, "", true)
    } else {
      if (full_sync_always) {
        // full sync — izbrauc cauri visiem ierakstiem ar kursoru
        migrateFullSlurpstein(read, write)
      } else {
        val where = getWhere(read, write, opts)
        migrateClassic(read, write, where, false)
      }
    }
  }

  def migrateClassic(read: Connection, write: Connection, where: String, straight_inserts: Boolean): Long = {
    // pārdzen $read -> $write ierakstus, kas atbilst $where parametriem
    // $straight_inserts: optimizācija, ja zināms, ka drīkst ierakstus insertot bez eksistences pārbaudīšanas
    //   normāli vajag $straight_inserts = false

    val order = pk_field._3
    val n_total = Sql.get_int(read, s"select count(*) from $remoteSource $where")
    val batchsize = 0L // tickeris nosaka dinamisku batchsize, lai būtu ~20 sekundēs viens tikšķis

    val ins = new NamedStatement(write, insertSql)
    val upd = new NamedStatement(write, updateSql)
    // Printer.msg(s"select count(*) from $remoteSource $where")

    val ticker = new Ticker(s"Migrating $remoteSource -> $localDest", n_total, batchsize)

    if (n_total > 0) {

      val s = read.createStatement
      s.setFetchSize(fetch_size)
      val rs = s.executeQuery(s"select ${remoteFieldsNeeded.mkString(", ")} from $remoteSource $where order by $order")

      while (rs.next) {

        val mod = if (straight_inserts) ins else {
          var is_new_rec = true
          val ss = write.createStatement
          val pk = rs.getLong(pk_field._1)
          val rss = ss.executeQuery(s"select ${pk_field._1} from $localDest where ${pk_field._1}=$pk")
          if (rss.next) {
            is_new_rec = false
          }
          rss.close
          ss.close
          if (is_new_rec) ins else upd
        }

        {
          fieldDef.foreach {
            case (f, fieldType, remoteField) => {
              fieldType match {
                case "pk" => mod.setLong("sync_id", rs.getLong(f))
                case "int" => {
                  val v = rs.getLong(f)
                  if (rs.wasNull) {
                    mod.setLongNull(f)
                  } else {
                    mod.setLong(f, v)
                  }

                }
                case "string" => mod.setString(f, rs.getString(f))
                case "date" => mod.setDate(f, rs.getDate(f))
                case "decimal" => mod.setDecimal(f, rs.getBigDecimal(f))
                case "vertiba" => 
                  val bd:java.math.BigDecimal = rs.getBigDecimal(f)
                  if (rs.wasNull) {
                    mod.setLongNull(f)
                  } else {
                    val v = BigDecimal(bd);
                    mod.setDecimal(f, v.bigDecimal)
                  }
                case "decimal_20_8" => mod.setDecimal(f, rs.getBigDecimal(f))
                case "decimal_22_0" => mod.setDecimal(f, rs.getBigDecimal(f))
                case "yn" => mod.setBoolean(f, rs.getString(f) == "Y")
                case "01" => mod.setBoolean(f, rs.getString(f) == "1")
                case "boolean" => mod.setBoolean(f, rs.getBoolean(f))
                case "legacy_datetag" => mod.setString(f, rs.getString(f))
                case "datetag" => mod.setTimestamp(f, Util.riga2utc(rs.getString(f)))
                case "datetime" => mod.setTimestamp(f, Util.riga2utc(rs.getString(f)))
                case default => throw new Exception(s"Unsupported field type $fieldType")
              }
            }
          }
          mod.addBatch
        }

        if (ticker.tick) {
          ins.runBatch
          upd.runBatch
          Sql.commit(write)
        }

      }

      ins.runBatch
      upd.runBatch
      Sql.commit(write)

      ins.close
      upd.close
      rs.close

      Printer.msg(s"Migrating $remoteSource -> $localDest: done ($n_total)")
    }

    n_total
  }

  def migrateFullSlurpstein(vzd: Connection, local: Connection) : Unit = {
    // aktualizē remotes dzēstos ierakstus, izejot cauri diviem kursoriem

    Printer.msg(s"Verify $localDest")

    val local_s = local.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
    local_s.setFetchSize(100_000)

    val remote_s = vzd.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
    remote_s.setFetchSize(100_000)

    val remote_rs = remote_s.executeQuery(s"select ${pk_field._3} from $remoteSource order by ${pk_field._3}")
    val local_rs = local_s.executeQuery(s"select ${pk_field._1} from $localDest order by 1")

    var ours : Option[Long] = None
    var theirs : Option[Long] = None

    if (remote_rs.next) theirs = Some(remote_rs.getLong(1))
    if (local_rs.next) ours = Some(local_rs.getLong(1))

    val to_delete = scala.collection.mutable.Set[Long]()
    val to_insert = scala.collection.mutable.Set[Long]()

    var n = 0
    while (ours != None && theirs != None) {

      n += 1
      if (n % 1_000_000 == 0) {

        Printer.msg(s"Read ${n}, +${to_insert.size} -${to_delete.size}...")
        if (to_insert.size > 0) {

          
          // migrate missing records in small batches
          to_insert.grouped(1000).foreach( elts => {
            val where = s"where ${pk_field._3} in (${elts.mkString(", ")})"
            Sql.commit_disable()
            migrateClassic(vzd, local, where, true)
            Sql.commit_enable()
          } )
          to_insert.clear()
        }

        if (to_delete.size > 0) {
          Sql.run(local, s"delete from $localDest where ${pk_field._1} in (${to_delete.mkString(", ")})")
          to_delete.clear()
        }

      }

      if (ours.get < theirs.get) {
        // mums ir dati, kas nav viņam
        // Printer.msg(s" - ${ours.get}")
        to_delete += ours.get
        ours = if (local_rs.next) Some(local_rs.getLong(1)) else None
      } else if (ours.get > theirs.get) {
        // mums iztrūkst dati
        // Printer.msg(s" + ${theirs.get} (!!!)")
        to_insert += theirs.get
        theirs = if (remote_rs.next) Some(remote_rs.getLong(1)) else None

      } else if (ours.get == theirs.get) {

        theirs = if (remote_rs.next) Some(remote_rs.getLong(1)) else None
        ours = if (local_rs.next) Some(local_rs.getLong(1)) else None

      }

    }

    if (ours == None && theirs != None) {
      // viņiem vēl ir extra ieraksti
      while (remote_rs.next) to_insert += remote_rs.getLong(1)
    }
    if (ours != None && theirs == None) {
      // mums vēl ir extra ieraksti
      while (local_rs.next) to_delete += local_rs.getLong(1)
    }

    if (to_insert.size > 0) {
      to_insert.grouped(1000).foreach( elts => {
        val where = s"where ${pk_field._3} in (${elts.mkString(", ")})"
        Sql.commit_disable()
        migrateClassic(vzd, local, where, true)
        Sql.commit_enable()
      } )
    }

    if (to_delete.size > 0) {
      Sql.run(local, s"delete from $localDest where ${pk_field._1} in (${to_delete.mkString(", ")})")
    }

    Sql.commit(local)

    local_rs.close
    remote_rs.close

  }


}

