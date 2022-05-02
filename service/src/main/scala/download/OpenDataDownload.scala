package lv.addresses.service.download

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.headers.`Last-Modified`
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Source}
import lv.addresses.service.config.Configs
import lv.addresses.service.{AddressConfig, AddressService, Boot}

import java.io.File
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object OpenDataDownload {

  private case object Synchronize

  def initialize(config: Configs.OpenData) = {
    import config._

    import AddressService._

    import Boot._
    val downloader = new Downloader()
    val initialDelay = 1.minute

    as.log.info(s"Open data address synchronization job will start in $initialDelay and will run " +
      s"every ${AddressConfig.updateRunInterval}")

    Source.tick(initialDelay, AddressConfig.updateRunInterval, Synchronize).runForeach { _ =>
      as.log.info("Starting address open data synchronization job.")
      Future.sequence(List(
        downloader.download(url, directory, AddressFilePrefix, ".zip"),
        downloader.download(historyUrl, directory, AddressHistoryFilePrefix, ".zip"),
      )).map {
        case List(IOResult(ac, _), IOResult(hc, _)) =>
          if (ac >= 0)
            system.log.info(s"Successfuly downloaded $ac bytes from $url")
          if (hc >= 0)
            system.log.info(s"Successfuly downloaded $hc bytes from $historyUrl")
          if (ac > 0 || hc > 0) {
            system.log.info(s"Deleting old address files...")
            deleteOldFiles(directory, AddressFilePattern, AddressHistoryFilePattern)
            publish(MsgEnvelope("check-new-version", CheckNewVersion))
          }
        case err => sys.error(s"Unexpected download result: $err")
      }.failed.foreach {
        as.log.error(_, s"Error occured while downloading address from open data portal.")
      }
    }
  }

  class Downloader {
    implicit val system = ActorSystem("open-data-ar-download")
    implicit val ec     = system.dispatcher

    def download(url: String, destDir: String, prefix: String, suffix: String) = {
      Http().singleRequest(Get(url)).flatMap { resp =>
        val fileName = resp.header[`Last-Modified`]
          .map(_.date.toIsoDateTimeString().replaceAll("[-:]", "_"))
          .map(prefix + _ + suffix)
          .getOrElse(sys.error(s"Last-Modified header was not found in response for '$url'." +
            s" Cannot set address data file name."))
        val destFile = new File(destDir, fileName)
        if (destFile.exists()) {
          system.log.info(s"Address file already exists: $destFile")
          resp.discardEntityBytes()
          Future.successful(IOResult(-1))
        } else {
          system.log.info(s"Downloading file: $destFile")
          //store data first into temp file, so that loading process does not pick up partial file
          val tmpFile = File.createTempFile(prefix, ".tmp", new File(destDir))
          resp.entity.dataBytes.runWith(FileIO.toPath(tmpFile.toPath)).andThen {
            case Success(IOResult(_, Success(_))) => tmpFile.renameTo(destFile)
          }
        }
      }
    }
  }
}
