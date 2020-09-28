package lv.addresses

import java.sql.Connection
import java.sql.DriverManager

import scala.util.{Failure, Success, Try}
import scala.collection.immutable.ListMap
import lv.uniso.migration.{Lock, Migrator, Printer}

import scala.annotation.tailrec

object M {
  val migrations = List(

    ("AK.ARG_ADRESE", "arg_adrese", List(
      // 1.5m recs

      // local_name,  datatype,   remote_name
      ("adr_cd",       "pk",       "adr_cd"),   // Adreses kods - ārējā saite uz tabulu ART_VIETA, ART_NLIETA vai ART_DZIV, atkarībā no lauka TIPS_CD
      ("tips_cd",      "int",      "tips_cd"),  // Adresācijas objekta tips - ārējā saite uz tabulu ART_KONST
      ("statuss",      "string",   "statuss"),  // Adresācijas objekta statuss (EKS - eksistē; DEL - likvidēta; ERR - kļūdaina)
      ("apstipr",      "string",   "apstipr"),  // Adresācijas objekta apstiprinājuma pakāpe (Y - apstiprināta; N - neapstiprināta)
      ("apst_pak",     "int",      "apst_pak"), // Apstiprinājuma pakāpe
      ("std",          "string",   "std"),      // Adreses standarta pieraksts
      ("vkur_cd",      "int",      "vkur_cd"),  // Adresācijas objekta, kur ietilpst, kods
      ("t1",           "string",   "t1"),       // Adreses standarta pieraksta fragments
      ("t2",           "string",   "t2"),       // -"-
      ("t3",           "string",   "t3"),       // -"-
      ("t4",           "string",   "t4"),       // -"-
      ("t5",           "string",   "t5"),       // -"-
      ("t6",           "string",   "t6"),       // -"-

      ("dat_sak",      "datetime", "dat_sak"),  // Datums, no kura adrese ir aktuāla
      ("dat_beig",     "datetime", "dat_beig"), // Datums, līdz kuram vieta eksistēja

      ("dat_mod",      "datetag",  "dat_mod"),  // Pēdējās modifikācijas datums
    )),

    ("AK.ARG_ADRESE_ARH", "arg_adrese_arh", List(
      // 3m recs
      ("id",           "pk",       "id"),
      ("adr_cd",       "int",      "adr_cd"),
      ("tips_cd",      "int",      "tips_cd"),
      ("statuss",      "string",   "statuss"),
      ("apstipr",      "string",   "apstipr"),
      ("apst_pak",     "int",      "apst_pak"),
      ("std",          "string",   "std"),
      ("vkur_cd",      "int",      "vkur_cd"),
      ("t1",           "string",   "t1"),
      ("t2",           "string",   "t2"),
      ("t3",           "string",   "t3"),
      ("t4",           "string",   "t4"),
      ("t5",           "string",   "t5"),
      ("t6",           "string",   "t6"),

      ("dat_sak",      "datetime", "dat_sak"),
      ("dat_beig",     "datetime", "dat_beig"),

      ("dat_mod",      "datetag",  "dat_mod"),
    )),

    ("AK.ART_VIETA", "art_vieta", List(
      // 30k recs
      ("kods",         "pk",        "kods"),      // Vietas kods (sekvence - VIETA_SEQ)
      ("tips_cd",      "int",       "tips_cd"),   // Adresācijas objekta tipa kods - ārējā atslēga uz tabulu ART_KONST
      ("apstipr",      "string",    "apstipr"),   // Nosaka vai informācija par vietu ir apstiprināta, vērtības (null; not null)
      ("apst_pak",     "int",       "apst_pak"),  // Datu apstiprinājuma pakāpe (ticamība)
      ("statuss",      "string",    "statuss"),   // Nosaka vietas statusu (EKS - eksistē; DEL - likvidēta; ERR - kļūdaina)
      ("vkur_cd",      "int",       "vkur_cd"),   // Denormalizētais lauks - norāda uz vietu, kurā dotā vieta tieši ietilpst - ārējā atslēga uz tabulu ART_VIETA
      ("vkur_tips",    "int",       "vkur_tips"), // Denormalizētais lauks - norāda uz vietas, kurā dotā vieta tieši ietilpst, tipu - ārējā atslēga uz tabulu ART_KONST
      ("nosaukums",    "string",    "nosaukums"), // Denormalizētais lauks - vietas pilnais nosaukums (pamatnosaukums + nomenklatūras vārds)
      ("sort_nos",     "string",    "sort_nos"),  // Denormalizētais lauks - vietas pamatnosaukuma kārtošanas nosaukums
      ("atrib",        "string",    "atrib"),     // Denormalizētais lauks - viena no vietas atribūta vērtībām (atkarībā no vietas tipa)

      ("dat_sak",      "datetime", "dat_sak"),    // Datums,  no kura vieta ir vai bija eksistējoša
      ("dat_beig",     "datetime", "dat_beig"),   // Datums, kad vieta ir tikusi likvidēta

      ("dat_mod",      "datetag",  "dat_mod"),    // Datums, kad pēdējo reizi ir ticis modificēts dotais raksts vai kāds ar doto vietu saistīts tās atribūts citās datnēs
    )),

    ("AK.ART_NLIETA", "art_nlieta", List(
      // 500k recs
      ("kods",         "pk",        "kods"),      // Nekustamās lietas kods (sekvence - VIETA_SEQ)
      ("tips_cd",      "int",       "tips_cd"),   // Adresācijas objekta tips - nekustama lieta (ārējā saite uz tabulu ART_KONST)
      ("statuss",      "string",    "statuss"),   // Adresācijas objekta statuss (EKS - eksistē; DEL - likvidēta; ERR - kļūdaina)
      ("apstipr",      "string",    "apstipr"),   // Vai adresācijas objekta adrese ir apstiprināta (Y; null)
      ("apst_pak",     "int",       "apst_pak"),  // Datu apstiprinājuma pakāpe (ticamība)
      ("vkur_cd",      "int",       "vkur_cd"),   // Vietas, kur ietilpst nekustamā lieta, kods - ārējā saite uz tabulu ART_VIETA. Denormalizētais lauks no tabulas ART_NLSAITE
      ("vkur_tips",    "int",       "vkur_tips"), // Vietas, kur ietilpst nekustamā lieta, tipa kods - ārējā saite uz datni ART_KODIFIKATORS. Denormalizētais lauks no tabulas ART_NLSAITE
      ("nosaukums",    "string",    "nosaukums"), // Nekustamās lietas nosaukums (denormalizētais lauks no tabulām ART_NOM_VARDS un ART_PNOS)
      ("sort_nos",     "string",    "sort_nos"),  // Nekustamās lietas kārtošanas nosaukums (denormalizētais lauks no datnēm ART_NOM_VARDS un ART_PNOS)
      ("atrib",        "string",    "atrib"),     // Pasta indekss - denormalizētais lauks no tabulas ART_VIETA (ATRIB) p.n. apk. terit.
      ("pnod_cd",      "int",       "pnod_cd"),   // Pasta nod. apk. terit. Kods - denormalizētais lauks, saite uz tabulu ART_VIETA

      ("for_build",    "string",    "for_build"), // Apbūvei paredzēts zemes gabals

      ("dat_sak",      "datetime", "dat_sak"),    // No kura datuma nekustamā lieta eksistē
      ("dat_beig",     "datetime", "dat_beig"),   // Līdz kuram datumam nekustamā lieta eksistēja

      ("dat_mod",      "datetag",  "dat_mod"),    // Datums, kad pēdējo reizi modificēti nekustamās lietas atribūti
    )),

    ("AK.ART_EKA_GEO", "art_eka_geo", List(
      // 500k recs
      ("mslink",       "pk",        "mslink"),    // Sasaiste ar grafisko objektu L50_LINES
      ("vieta_cd",     "int",       "vieta_cd"),  // Saite uz tabulu ART_NLIETA
      ("koord_x",      "decimal",   "koord_x"),   // Adreses X koordināte
      ("koord_y",      "decimal",   "koord_y"),   // Adreses Y koordināte
    )),
  )
}

object Updater {

  def migrator (ps: Tuple3[String, String, List[Tuple3[String, String, String]]], opts:OptMap) = {
      new Migrator(ps._1, ps._2, ps._3)
  }

  var migrations = M.migrations

  val default_opts = Map(
    "local.driver"     -> "org.postgresql.Driver",
    "local.connection" -> "jdbc:postgresql://127.0.0.1:5432/adreses?rewriteBatchedStatements=true",
    "local.username" -> "postgres",
    "local.password" -> "",

    "vzd.driver"     -> "oracle.jdbc.OracleDriver",
    "vzd.connection" -> """jdbc:oracle:thin:@(
      DESCRIPTION=(ADDRESS_LIST=
        (ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=1630))
        (ADDRESS=(PROTOCOL=TCP)(HOST=10.195.10.6)(PORT=1521))
      )(SOURCE_ROUTE=yes)(CONNECT_DATA=(SERVICE_NAME=KRRITEST.VZD.GOV.LV))
    )""",
    "vzd.username"   -> "vraa_amk_izstr",
    "vzd.password"   -> "",
    "lockfile"        -> "/tmp/addresses-vzd-receive.lock",

    "verify" -> "false"
  )

  var local: Option[Connection] = None
  var vzd: Option[Connection] = None

  def usage() : Unit = {
    val supported = M.migrations.map(_._2).mkString(", ")

  System.err.println(s"""
Updater retrieves up-to-date address data from VZD address register.

Usage:
  vzd-receive [OPTION]... [table_to_migrate table_to_migrate ...]


Destination connection options:
  --driver DRIVER               JDBC driver to connect to destination database
                                (default: "${default_opts("local.driver")}")
  --connection CONNECTION       JDBC connection string for destination connection
                                (default: "${default_opts("local.connection")}")
  --username USERNAME           username to connect to destination
                                (default: "${default_opts("local.username")}")
  --password PASSWORD           connection password to destination

VZD connection options:
  --vzd-driver DRIVER          JDBC driver to connect to VZD
                                (default: "${default_opts("vzd.driver")}")
  --vzd CONNECTION
  --vzd-connection CONNECTION  JDBC connection string for VZD connection
  --vzd-username USERNAME      username to connect to VZD
                                (default: "${default_opts("vzd.username")}")
  --vzd-password PASSWORD      connection password to VZD

Transfer options:
  --lock LOCKFILE               use alternate lockfile
                                (default: ${default_opts("lockfile")})
  --verify                      verify data integrity (ids only)

Supported migration tables:
  $supported

Examples:

   Ordinary sync:
   % ./vzd-receive

   Update arg_adrese only:
   % ./vzd-receive arg_adrese

    """.trim + "\n")
  }

  type OptMap = Map[String, String]

  var custom_mode = false

  @tailrec
  def gatherOpts(opts: OptMap, args: List[String]) : Option[OptMap] = {
    args match {
      case Nil => Some(opts)

      case "-h"                    :: rest => None
      case "--help"                :: rest => None

      case "--driver"         :: s :: rest => gatherOpts(opts + ("local.driver" -> s), rest)
      case "--connection"     :: s :: rest => gatherOpts(opts + ("local.connection" -> s), rest)
      case "--user"           :: s :: rest => gatherOpts(opts + ("local.username" -> s), rest)
      case "--username"       :: s :: rest => gatherOpts(opts + ("local.username" -> s), rest)
      case "--password"       :: s :: rest => gatherOpts(opts + ("local.password" -> s), rest)
      case "--passwd"         :: s :: rest => gatherOpts(opts + ("local.password" -> s), rest)

      case "--vzd-driver"     :: s :: rest => gatherOpts(opts + ("vzd.driver" -> s), rest)
      case "--vzd"            :: s :: rest => gatherOpts(opts + ("vzd.connection" -> s), rest)
      case "--vzd-connection" :: s :: rest => gatherOpts(opts + ("vzd.connection" -> s), rest)
      case "--vzd-user"       :: s :: rest => gatherOpts(opts + ("vzd.username" -> s), rest)
      case "--vzd-username"   :: s :: rest => gatherOpts(opts + ("vzd.username" -> s), rest)
      case "--vzd-password"   :: s :: rest => gatherOpts(opts + ("vzd.password" -> s), rest)
      case "--vzd-passwd"     :: s :: rest => gatherOpts(opts + ("vzd.password" -> s), rest)

      case "--lock"    :: lockfile :: rest => gatherOpts(opts + ("lockfile" -> lockfile), rest)
      case "--verify"              :: rest => gatherOpts(opts + ("verify" -> "true"), rest)

      case other :: rest => 
        val elem = M.migrations.find(s => s._2 == other)
        elem match {
          case Some(e) => 
            if (custom_mode == false) {
              custom_mode = true
              migrations = Nil
            }
            migrations = e :: migrations
            gatherOpts(opts, rest)
          case None => 
            Printer.msg(s"Unknown option ${other}.\n")
            None
        }
    }
  }

  def fatal(text: String) = new Exception(text)

  def connect(opts: OptMap, base: String): Option[Connection] = {

    val driver = opts(s"$base.driver")
    val connection = opts(s"$base.connection")
    val username = opts(s"$base.username")
    val password = opts(s"$base.password")

    Printer.msg(s"Connecting to $connection...")

    Try(Class.forName(driver)) match {
      case Failure(s) => 
        throw fatal(s"Unable to load driver $driver")
      case default => ()
    } 

    Try(DriverManager.getConnection(connection, username, password)) match {
      case Failure(s) =>
        Printer.msg(s.getMessage)
        throw fatal(s"Unable to connect to $connection as $username")
      case Success(conn) =>
        Printer.msg(s"Connection successful")
        conn.setAutoCommit(false)
        Some(conn)
    }
  }

  def importWith(opts: OptMap) : Unit = {

    val lock = new Lock(opts("lockfile"))

    local = connect(opts, "local")
    vzd = connect(opts, "vzd")


    {

      val ms = migrations.reverseIterator.map( m => migrator(m, opts) )

      ms.foreach( mig => {
        Printer.msg(s"Migrate ${mig.source} -> ${mig.target}")
        // mig.rebuild( local.get, opts("truncate") == "true" ) 
        mig.rebuild(local.get, truncate = false)
        mig.migrate(vzd.get, local.get, opts)
      })

    }

    lock.release()
  }

  def main(args: Array[String]) : Unit = {

    gatherOpts(default_opts, args.toList) match {
      case Some(opts) => Try(importWith(opts)) match {
        case Success(_) => ()
        case Failure(e) => Printer.msg(e.getMessage); e.printStackTrace()
      }
      case None => usage()
    }

  }


}
