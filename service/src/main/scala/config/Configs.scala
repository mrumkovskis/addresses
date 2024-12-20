package lv.addresses.service.config

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import org.tresql.{LogTopic, Query, Resources, SimpleCache}

import java.nio.file.{Files, Path}
import java.sql.DriverManager
import java.time.{LocalDate, LocalDateTime}
import scala.util.{Failure, Using}
import scala.util.matching.Regex

object Configs {
  sealed trait VARConfig {
    val AddressesPostfix = "addresses"
    val IndexPostfix = "index"
    def directory: String
    def version: String
  }

  /** @param name           - full file name on disk.
   *  @param structureName  - last component from open data file url indicating csv file sturcture */
  case class OpenDataAddressFile(name: String, structureName: String)

  case class OpenData(urls: List[String],
                      historyUrls: List[String],
                      directory: String,
                      historySince: LocalDate) extends VARConfig {
    def addressFiles: List[OpenDataAddressFile] = files(urls)
    def historyAddressFiles: List[OpenDataAddressFile] = files(historyUrls)
    def version: String =
      addressFiles
        .map { f =>
          val fn = f.name
          fn.substring(0, fn.indexOf("."))
        }
        .sorted
        .lastOption
        .orNull

    def addressFilePattern(fileName: String): String = """\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.""" + fileName
    def fileNameFromUrl(url: String) = url.substring(url.lastIndexOf("/") + 1)

    private def currentFile(pattern: String): String = {
      val regex = new Regex(pattern)
      Files
        .list(Path.of(directory))
        .map(_.getFileName.toString)
        .filter(regex.matches)
        .toArray.map(_.asInstanceOf[String])
        .sorted
        .lastOption
        .orNull
    }
    private def files(urls: List[String]) = {
      urls.map { url =>
        val fn = fileNameFromUrl(url)
        val currentFileName = currentFile(addressFilePattern(fn))
        if (currentFileName == null) null else OpenDataAddressFile(name = currentFileName, structureName = fn)
      }.filter(_ != null)
    }
  }

  case class Db(driver: String,
                url: String,
                user: String,
                password: String,
                directory: String) extends VARConfig {
    val DbDataFilePrefix = "VZD_AR_"
    def version: String =
      lastSyncTime
        .map(DbDataFilePrefix + _.toString.replace(':', '_').replace('.', '_'))
        .orNull

    lazy val tresqlResources = new Resources {
      val infoLogger = Logger(LoggerFactory.getLogger("org.tresql"))
      val tresqlLogger = Logger(LoggerFactory.getLogger("org.tresql.tresql"))
      val sqlLogger = Logger(LoggerFactory.getLogger("org.tresql.db.sql"))
      val varsLogger = Logger(LoggerFactory.getLogger("org.tresql.db.vars"))
      val sqlWithParamsLogger = Logger(LoggerFactory.getLogger("org.tresql.sql_with_params"))

      override val logger = (m, params, topic) => topic match {
        case LogTopic.sql => sqlLogger.debug(m)
        case LogTopic.tresql => tresqlLogger.debug(m)
        case LogTopic.params => varsLogger.debug(m)
        case LogTopic.sql_with_params => sqlWithParamsLogger.debug(sqlWithParams(m, params))
        case LogTopic.info => infoLogger.debug(m)
        case _ => infoLogger.debug(m)
      }

      override val cache = new SimpleCache(4096)

      def sqlWithParams(sql: String, params: Map[String, Any]) = params.foldLeft(sql) {
        case (sql, (name, value)) => sql.replace(s"?/*$name*/", value match {
          case _: Int | _: Long | _: Double | _: BigDecimal | _: BigInt | _: Boolean => value.toString
          case _: String | _: java.sql.Date | _: java.sql.Timestamp => s"'$value'"
          case null => "null"
          case _ => value.toString
        })
      }
    }

    private def lastSyncTime: Option[LocalDateTime] = {
      Class.forName(driver)
      Using(DriverManager.getConnection(url, user, password)) { conn =>
        implicit val res = tresqlResources withConn conn
        Query("(art_vieta { max (sync_synced) d } +" +
          "art_nlieta { max (sync_synced) d } +" +
          "art_dziv { max (sync_synced) d }) { max(d) }").unique[LocalDateTime]
      }.recoverWith {
        case e: Exception =>
          Logger(LoggerFactory.getLogger("org.tresql")).error("Error getting last sync time", e)
          Failure(e)
      }
    }.toOption
  }
}
