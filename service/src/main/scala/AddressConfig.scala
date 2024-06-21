package lv.addresses.service

import loader.{DbLoader, OpenDataLoader}
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
      import scala.jdk.CollectionConverters._
      Configs.OpenData(
        odConf.getStringList("urls").asScala.toList,
        odConf.getStringList("history-urls").asScala.toList,
        odConf.getString("save-to-dir"),
        Try(odConf.getString("history-since"))
          .map(LocalDate.parse(_))
          .toOption
          .orNull,
      )
    } else {
      sys.error("Neither VZD.open-data nor VZD.db configuration found")
    }
  }

  val addressLoader: () => Addresses = () => addressConfig match {
    case c: Configs.Db        =>
      DbLoader.loadAddresses(c.driver, c.url, c.user, c.password, c.tresqlResources)
    case c: Configs.OpenData  =>
      OpenDataLoader.loadAddresses(
        c.addressFiles.map(af => new File(c.directory, af).getPath),
        c.historyAddressFiles.map(ahf => new File(c.directory, ahf).getPath),
        c.historySince
      )
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
