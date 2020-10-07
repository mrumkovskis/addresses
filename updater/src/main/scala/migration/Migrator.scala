package lv.uniso.migration

import java.sql.Connection

import java.time.format.DateTimeFormatter
import java.time.ZoneId

class Migrator (remoteSource:String, localDest:String, fieldDef:List[Tuple3[String, String, String]]) {

  val batch_size = 50000
  val fetch_size = 5000

  type OptMap = Map[String, String]

  def source: String = remoteSource
  def target: String = localDest

  val orafmt = "YYYY-MM-DD HH24:MI:SS"

  lazy val pk_field: (String, String, String) = fieldDef.find(_._2 == "pk").get

  def full_sync_always: Boolean = {
    fieldDef.find( s => (s._2 == "datetag")) == None
  }

  def datetag_local: String = {
    (fieldDef.find( s => (s._2 == "datetag")).get)._1
  }
  def datetag_remote: String = {
    (fieldDef.find( s => (s._2 == "datetag")).get)._3
  }

  def insertSql(): String = {

    val fields = fieldDef.filter(fd => fd._2 != "pk")
    val fds = s"${pk_field._1}, sync_synced, " + fields.map(_._1).mkString(", ")
    val ins = ":sync_id, current_timestamp, "       + fields.map(fd => s":${fd._1}").mkString(", ")

    // s"insert into $localDest ($fds) values ($ins) on conflict do nothing"
    // oh come on, 9.5 with "on conflict" support was released in jan-2016
    s"insert into $localDest ($fds) values ($ins)"

  }

  def updateSql(): String = {

    val fields = fieldDef.filter(fd => fd._2 != "pk")
    val upds = fields.map(fd => s"${fd._1}=:${fd._1}").mkString(", ")

    s"update $localDest set $upds, sync_synced=current_timestamp where ${pk_field._1}=:sync_id"

  }

  def remoteFieldsNeeded: List[String] = {
    fieldDef.map{
      case (local, t, fd) => 
        if (t == "datetag" || t == "datetime") {
          s"to_char($fd, '$orafmt') as $local"
        } else {
          s"$fd as $local"
        }
    }
  }

  def createTableSql: String = {
    val defs = fieldDef.map( fd => {

      val sqlType = fd._2 match {
        case "pk" => "bigint primary key"
        case "fk" => "bigint"
        case "int" => "int"
        case "string" => "varchar"
        case "yn" => "boolean"
        case "01" => "boolean"
        case "boolean" => "boolean"
        case "decimal" => "decimal(10, 3)"
        case "date" => "date"
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
      ps.close()
      write.commit()
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

  def getWhere(vzd: Connection, local: Connection, opts: OptMap): String = {
    val dt_max_ts = Sql.get_instant(local, s"select max(${datetag_local}) from $localDest").get
    // warning, te, iespējams, slēpjas velni ar laikazonām
    val dt_max = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Europe/Riga")).format(dt_max_ts)
    Printer.info(s"Fetching new records since ${dt_max}")
    s"where ${datetag_remote} > to_date('${dt_max}', '$orafmt')"
  }

  def migrate(read: Connection, write: Connection, opts: OptMap) : Unit = {

    val n_existing = Sql.get_int(write, s"select count(*) from $localDest")

    if (opts("verify") == "true") {
      migrateFullSlurpstein(read, write)
    } else if (n_existing == 0) {
      // pārdzenam 1:1 optimizētā režīmā (straight_inserts = true)
      Printer.debug(s"Destination empty, running fast copy")
      migrateClassic(read, write, "", straight_inserts = true)
    } else {
      if (full_sync_always) {
        // full sync — izbrauc cauri visiem ierakstiem ar kursoru
        migrateFullSlurpstein(read, write)
      } else {
        val where = getWhere(read, write, opts)
        migrateClassic(read, write, where, straight_inserts = false)
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

    val ins = new NamedStatement(write, insertSql())
    val upd = new NamedStatement(write, updateSql())

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
          rss.close()
          ss.close()
          if (is_new_rec) ins else upd
        }

        {
          fieldDef.foreach {
            case (f, fieldType, remoteField) =>
              fieldType match {
                case "pk" => mod.setLong("sync_id", rs.getLong(f))
                case "fk" | "int" =>
                  val v = rs.getLong(f)
                  if (rs.wasNull) mod.setLongNull(f)
                  else mod.setLong(f, v)
                case "string" => mod.setString(f, rs.getString(f))
                case "date" => mod.setDate(f, rs.getDate(f))
                case "decimal" => mod.setDecimal(f, rs.getBigDecimal(f))
                case "yn" => mod.setBoolean(f, rs.getString(f) == "Y")
                case "01" => mod.setBoolean(f, rs.getString(f) == "1")
                case "boolean" => mod.setBoolean(f, rs.getBoolean(f))
                case "legacy_datetag" => mod.setString(f, rs.getString(f))
                case "datetag" => mod.setTimestamp(f, Util.riga2utc(rs.getString(f)))
                case "datetime" => mod.setTimestamp(f, Util.riga2utc(rs.getString(f)))
                case default => throw new Exception(s"Unsupported field type $fieldType")
              }
          }
          mod.addBatch()
        }

        if (ticker.tick) {
          ins.runBatch()
          upd.runBatch()
          Sql.commit(write)
        }

      }

      ins.runBatch()
      upd.runBatch()
      Sql.commit(write)

      ins.close()
      upd.close()
      rs.close()

      Printer.info(s"Migrating $remoteSource -> $localDest: done ($n_total)")
    }

    n_total
  }

  def migrateFullSlurpstein(vzd: Connection, local: Connection) : Unit = {
    // aktualizē remotes dzēstos ierakstus, izejot cauri diviem kursoriem

    Printer.info(s"Verify $localDest")

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

        Printer.debug(s"Read ${n}, +${to_insert.size} -${to_delete.size}...")
        if (to_insert.nonEmpty) {

          
          // migrate missing records in small batches
          to_insert.grouped(1000).foreach( elts => {
            val where = s"where ${pk_field._3} in (${elts.mkString(", ")})"
            Sql.commit_disable()
            migrateClassic(vzd, local, where, straight_inserts = true)
            Sql.commit_enable()
          } )
          to_insert.clear()
        }

        if (to_delete.nonEmpty) {
          Sql.run(local, s"delete from $localDest where ${pk_field._1} in (${to_delete.mkString(", ")})")
          to_delete.clear()
        }

      }

      if (ours.get < theirs.get) {
        // mums ir dati, kas nav viņam
        to_delete += ours.get
        ours = if (local_rs.next) Some(local_rs.getLong(1)) else None
      } else if (ours.get > theirs.get) {
        // mums iztrūkst dati
        to_insert += theirs.get
        theirs = if (remote_rs.next) Some(remote_rs.getLong(1)) else None

      } else if (ours.get == theirs.get) {

        theirs = if (remote_rs.next) Some(remote_rs.getLong(1)) else None
        ours = if (local_rs.next) Some(local_rs.getLong(1)) else None

      }

    }

    if (ours.isEmpty && theirs != None) {
      // viņiem vēl ir extra ieraksti
      while (remote_rs.next) to_insert += remote_rs.getLong(1)
    }
    if (ours != None && theirs.isEmpty) {
      // mums vēl ir extra ieraksti
      while (local_rs.next) to_delete += local_rs.getLong(1)
    }

    if (to_insert.nonEmpty) {
      to_insert.grouped(1000).foreach( elts => {
        val where = s"where ${pk_field._3} in (${elts.mkString(", ")})"
        Sql.commit_disable()
        migrateClassic(vzd, local, where, straight_inserts = true)
        Sql.commit_enable()
      } )
    }

    if (to_delete.nonEmpty) {
      Sql.run(local, s"delete from $localDest where ${pk_field._1} in (${to_delete.mkString(", ")})")
    }

    Sql.commit(local)

    local_rs.close()
    remote_rs.close()

  }


}

