package lv.addresses.service

import scala.util.Success
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings}
import java.io.File
import java.net.InetAddress

import AddressService._

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
  val addressFileDir: String = scala.util.Try(config.getString("VZD.ak-file"))
    .toOption
    .map(s => s.substring(0, Math.max(s.lastIndexOf('/'), 0)))
    .orNull

  def isFTPConfigured = !(Set(host, username, password, ftpDir, addressFileDir) contains null)
  def initialize = if (isFTPConfigured) {
    val FILE_PATTERN = new scala.util.matching.Regex(akFileNamePattern)
    val ftpSettings = FtpSettings(InetAddress.getByName(host))
      .withCredentials(FtpCredentials.create(username, password))
      .withBinary(true)
      .withPassiveMode(true)
    import Boot._ //make available actor system
    import scala.concurrent.duration._
    val initialDelay = 1.minute
    Source.tick(initialDelay, runInterval, Download).runForeach { _ =>
      val current = Option(addressFileName)
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
                if (current != "") {
                  as.log.info(s"Deleting old VZD address file: $current")
                  new File(addressFileDir + "/" + current).delete()
                }
                AddressService.publish(MsgEnvelope("check-new-version", CheckNewVersion))
              case err =>
                as.log.error(s"Error downloading file $remoteFile from ftp server to $tmp - $err")
            }
        }.failed.foreach {
          case err => as.log.error(err,
            s"Unable to list remote ftp files: ftp://$username@$host/$ftpDir")
        }
    }
    as.log.info(s"FTP downloader will start after $initialDelay and will run at $runInterval intervals")
  } else as.log.info("FTP downloader not started due to missing configuration.")
}
