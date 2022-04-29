package lv.addresses.service

import loader.{AKLoader, DbLoader, OpenDataLoader}
import lv.addresses.indexer.{Addresses, IndexFiles}
import lv.addresses.service.config.Configs

import java.io.File
import java.time.LocalDate
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.Try

object AddressConfig {
  val updateRunInterval: FiniteDuration = {
    val conf = com.typesafe.config.ConfigFactory.load
    val dur: Duration =
      if (conf.hasPath("VZD.update-run-interval"))
        Duration(conf.getString("VZD.update-run-interval"))
      else 1.hour
      FiniteDuration(dur.length, dur.unit)
  }
  val addressConfig: Configs.VARConfig = {
    val conf = com.typesafe.config.ConfigFactory.load
    if (conf.hasPath("VZD.db")) {
      val dbConf = conf.getConfig("VZD.db")
      Configs.Db(
        dbConf.getString("driver"),
        dbConf.getString("url"),
        dbConf.getString("user"),
        dbConf.getString("password"),
        dbConf.getString("index-dir")
      )
    } else if (conf.hasPath("VZD.open-data")) {
      val odConf = conf.getConfig("VZD.open-data")
      Configs.OpenData(
        odConf.getString("url"),
        odConf.getString("history-url"),
        odConf.getString("save-to-dir"),
        Try(odConf.getString("history-since"))
          .map(LocalDate.parse(_))
          .toOption
          .orNull,
      )
    } else {
      val akFilePathPattern =
        if (conf.hasPath("VZD.ak-file")) conf.getString("VZD.ak-file")
        else sys.error("address file setting 'VZD.ak-file' not found")
      val houseCoordFile = scala.util.Try(conf.getString("VZD.house-coord-file")).toOption.orNull
      val excludeList: Set[String] =
        if (conf.hasPath("VZD.exclude-list"))
          conf.getString("VZD.exclude-list").split(",\\s+").toSet
        else Set()
      Configs.AK(akFilePathPattern, houseCoordFile, excludeList)
    }
  }

  val addressLoader: () => Addresses = () => addressConfig match {
    case c: Configs.Db        =>
      DbLoader.loadAddresses(c.driver, c.url, c.user, c.password, c.tresqlResources)
    case c: Configs.OpenData  =>
      val af  = new File(c.directory, c.addressFile).getPath
      val ahf = new File(c.directory, c.addressHistoryFile).getPath
      OpenDataLoader.loadAddresses(af, ahf, c.historySince)
    case c: Configs.AK        =>
      val af  = new File(c.directory, c.addressFile).getPath
      AKLoader.loadAddresses(af, c.houseCoordFile, c.excludeList)
  }

  def indexFiles: IndexFiles =
    Option(addressConfig.version)
      .map { ver =>
        IndexFiles(
          addresses = new File(addressConfig.directory, s"$ver.${addressConfig.AddressesPostfix}"),
          index = new File(addressConfig.directory, s"$ver.${addressConfig.IndexPostfix}"),
        )
      }.orNull
}
