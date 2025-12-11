package lv.addresses.service.download

import akka.stream.IOResult
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import lv.addresses.service.config.Configs
import lv.addresses.service.{AddressConfig, AddressService, Boot}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object OpenDataAddressDownload {

  private case object Synchronize

  protected val logger = Logger(LoggerFactory.getLogger("lv.addresses.downloader"))

  def initialize(config: Configs.OpenData) = {
    import config._

    import AddressService._

    import Boot._
    val downloader = new OpenDataDownloader(logger)
    val initialDelay = 1.minute

    logger.info(s"Open data address synchronization job will start in $initialDelay and will run " +
      s"every ${AddressConfig.updateRunInterval}")

    Source.tick(initialDelay, AddressConfig.updateRunInterval, Synchronize).runForeach { _ =>
      logger.info("Starting address open data synchronization job.")
      Future.sequence((urls ++ historyUrls).map(url => downloader.download(url, directory, fileNameFromUrl(url))))
        .map (_.map { case OpenDataDownloadRes(url, tempFile, destFile, IOResult(count, _)) =>
          if (count > 0 && tempFile.renameTo(destFile)) {
            logger.info(s"Successfuly downloaded $count bytes from $url")
            val fn = fileNameFromUrl(url)
            logger.info(s"Deleting old address files for $fn ...")
            val oldFiles = deleteOldFiles(directory, addressFilePattern(fn))
            if (oldFiles.isEmpty) logger.info(s"No address files deleted for $fn.")
            else logger.info(s"Deleted address files for $fn - (${oldFiles.mkString(", ")})")
          } else logger.info(s"No bytes downloaded from $url")
          count
        }).map(counts => if (counts.exists(_ > 0)) publish(MsgEnvelope("check-new-version", CheckNewVersion)))
        .failed.foreach {
        logger.error(s"Error occured while downloading address from open data portal.", _)
      }
    }
  }
}
