package lv.addresses.service

import org.apache.commons.net.ftp.FTPClient
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.net.ftp.FTPConnectionClosedException
import org.apache.commons.net.ftp.FTP
import akka.actor.Actor
import akka.actor.Props

import AddressService._

object FTPDownload {

  case object Download

  val config = com.typesafe.config.ConfigFactory.load

  val connection = new FTPClient()
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
    val a =  as.actorOf(Props[FTPDownload])
    a ! Download
  } else as.log.info("FTP downloader not started due to missing configuration.")

}

class FTPDownload extends Actor {

  import FTPDownload._

  val FILE_PATTERN = new scala.util.matching.Regex(akFileNamePattern)

  //start scheduler
  private val initScheduler = context.system.scheduler.schedule(
    initializerRunInterval, initializerRunInterval, self, Download)

  as.log.info("FTP downloader started")

  def receive: Receive = {
    case Download => download
  }

  private def connect {
    as.log.debug("Attempting to connect to " + host)
    connection.connect(host)
    connection.login(username, password)
    connection.enterLocalPassiveMode()
    connection.setFileType(FTP.BINARY_FILE_TYPE)
    as.log.debug("Connection established!")
  }

  private def getFileNames: Array[String] = {
    val fileNames = connection.listNames(ftpDir)
    as.log.debug(s"Files on FTP server: ${fileNames.mkString(", ")}")
    fileNames.flatMap(FILE_PATTERN.findFirstIn(_).toList)
  }

  private def downloadNewest(fName: String) {
    val tmp = System.getProperty("java.io.tmpdir") + "/" + fName
    val output = new FileOutputStream(tmp)
    connection.retrieveFile(ftpDir + "/" + fName, output)
    output.close()
    new File(tmp).renameTo(new File(addressFileDir + "/", fName))
  }

  private def download = try {
    connect
    val zips = getFileNames
    as.log.debug(s"Address files found on FTP server: ${zips.mkString(", ")}")
    val current = Option(addressFileName)
      .map(fn => fn.substring(fn.lastIndexOf('/') + 1))
      .getOrElse("")
    var newest = current
    for (zip <- zips) if (zip > newest) newest = zip

    if (newest != current) {
      as.log.info("Found a newer VZD address file")
      as.log.info(s"Downloading file: $newest")
      downloadNewest(newest)
      as.log.info("Download finished!")

      if (current != "") {
        as.log.info(s"Deleting old VZD address file: $current")
        new File(addressFileDir + "/" + current).delete()
      }
    } else as.log.info(s"Already have the newest VZD address file: $current")
  } finally disconnect

  private def disconnect {
    connection.logout()
    connection.disconnect()
    as.log.debug("Disconnected from " + host)
  }

}
