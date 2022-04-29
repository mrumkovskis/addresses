package lv.addresses.service.download

import akka.stream.IOResult
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings}
import akka.stream.scaladsl.{FileIO, Source}
import lv.addresses.service.AddressService.{CheckNewVersion, MsgEnvelope, publish}
import lv.addresses.service.AddressService._
import lv.addresses.service.Boot._

import java.io.File
import java.net.InetAddress
import scala.util.Success

object FTPDownload {

  private case object Download

  val config = com.typesafe.config.ConfigFactory.load

  val host: String = if(config.hasPath("VZD.ftp.host"))
    config.getString("VZD.ftp.host") else null
  val username: String = if (config.hasPath("VZD.ftp.username"))
    config.getString("VZD.ftp.username") else null
  val password: String = if (config.hasPath("VZD.ftp.password"))
    config.getString("VZD.ftp.password") else null
  val ftpDir: String = if (config.hasPath("VZD.ftp.dir"))
    config.getString("VZD.ftp.dir") else null

  def isFTPConfigured = !(Set(host, username, password, ftpDir) contains null)
  def initialize(addressFileDir: String, fileNamePattern: String, currentFile: String) = if (isFTPConfigured) {
    import lv.addresses.service.AddressConfig.updateRunInterval
    val FILE_PATTERN = new scala.util.matching.Regex(fileNamePattern)
    val ftpSettings = FtpSettings(InetAddress.getByName(host))
      .withCredentials(FtpCredentials.create(username, password))
      .withBinary(true)
      .withPassiveMode(true)

    import scala.concurrent.duration._
    val initialDelay = 1.minute
    Source.tick(initialDelay, updateRunInterval, Download).runForeach { _ =>
      val current = Option(currentFile)
        .map(fn => fn.substring(fn.lastIndexOf('/') + 1))
        .getOrElse("")
      as.log.info(s"Checking for new address file on ftp server. Current address file: $current")
      Ftp.ls(ftpDir, ftpSettings)
        .filter(_.isFile)
        .map(_.name)
        .mapConcat(FILE_PATTERN.findFirstIn(_).filter(_ != current).toList)
        .fold(current)((cur_newest, cur) => if (cur > cur_newest) cur else cur_newest)
        .filter(_ != current)
        .runForeach { fName =>
          val remoteFile = ftpDir + "/" + fName
          val tmp = System.getProperty("java.io.tmpdir") + "/" + fName
          as.log.info("Found a newer VZD address file")
          as.log.info(s"Downloading file: $remoteFile")
          Ftp.fromPath(remoteFile, ftpSettings)
            .runWith(FileIO.toPath(java.nio.file.Paths.get(tmp))).onComplete {
              case Success(IOResult(count, Success(_))) =>
                new File(tmp).renameTo(new File(addressFileDir + "/", fName))
                as.log.info(s"Download finished, $count bytes processed!")
                as.log.info(s"Deleting old VZD address files...")
                deleteOldFiles(addressFileDir, fileNamePattern)
                publish(MsgEnvelope("check-new-version", CheckNewVersion))
              case err =>
                as.log.error(s"Error downloading file $remoteFile from ftp server to $tmp - $err")
            }
        }.failed.foreach {
          case err => as.log.error(err,
            s"Unable to list remote ftp files: ftp://$username@$host/$ftpDir")
        }
    }
    as.log.info(s"FTP downloader will start after $initialDelay and will run at $updateRunInterval intervals")
  } else as.log.info("FTP downloader not started due to missing configuration.")
}
