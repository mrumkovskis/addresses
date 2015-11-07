package lv.addresses.service

import org.apache.commons.net.ftp.FTPClient
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.net.ftp.FTPConnectionClosedException
import org.apache.commons.net.ftp.FTP
import AddressService._

object FTPManager {
  val config = com.typesafe.config.ConfigFactory.load
  val seperator = System.getProperty("file.separator")

  val connection = new FTPClient()
  val host: String = config.getString("VZD.ftp.host")
  val username: String = config.getString("VZD.ftp.username")
  val password: String = config.getString("VZD.ftp.password")
  val path: String = config.getString("VZD.ftp.path")
  val dir: String = config.getString("VZD.workspace")

  def connect(): Boolean = {
    try {
      as.log.info("Attempting to Connect to " + host)
      connection.connect(host)
      connection.login(username, password)
      connection.enterLocalPassiveMode()
      connection.setFileType(FTP.BINARY_FILE_TYPE)
      as.log.info("Connection established!")
      true
    } catch {
      case e: FTPConnectionClosedException => {
        as.log.error("Connection could not be established")
        false
      }
    }

  }

  def getZipNames(): Array[String] = {
    val fileNames = connection.listNames(path)
    for (file <- fileNames if (file.endsWith(".zip")))
      yield file.replace(path, "").replace("/", "")
  }

  def getCurrentZip(): String = {
    val workspace = new File(dir)
    if (!workspace.exists())
      workspace.createNewFile()
    var files = workspace.list()
    files = for (file <- files if (file.endsWith(".zip"))) yield file
    if (files.length == 0) "0"
    else files.head
  }

  def getDate(name: String): Int = {
    augmentString(name.replace("AK", "").replace(".zip", "")).toInt
  }

  def downloadNewest(fName: String) {
    val output = new FileOutputStream(dir + "/" + fName)
    connection.retrieveFile(path + "/" + fName, output)
    output.close()
  }

  def download(): String = {
    if (connect()) {

      val zips = getZipNames()
      val current = getCurrentZip()
      var newest = current
      for (zip <- zips) if (getDate(zip) > getDate(newest)) newest = zip

      if (newest != current) {
        as.log.info("Found a newer VZD Zip File")
        as.log.info("Downloading File: " + newest)
        downloadNewest(newest)
        as.log.info("Download Finished!")

        if (current != "0") {
          as.log.info("Deleting old VZD Zip File:" + current)
          new File(dir + seperator + current).delete()
        }
        disconnect()
        newest
      } else {
        as.log.info("Already have the newest VZD Zip File")
        disconnect()
        newest
      }
    } else null
  }

  def disconnect() {
    connection.logout()
    connection.disconnect()
    as.log.info("Disconnected from " + host)
  }

}
