package lv.addresses.service.download

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.{Get, Head}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.`Last-Modified`
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Source}
import com.typesafe.scalalogging.Logger
import lv.addresses.service.config.Configs
import lv.addresses.service.{AddressConfig, AddressService, Boot}
import org.slf4j.LoggerFactory

import java.io.File
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Success

object OpenDataDownload {

  private case object Synchronize

  protected val logger = Logger(LoggerFactory.getLogger("lv.addresses.downloader"))

  def initialize(config: Configs.OpenData) = {
    import config._

    import AddressService._

    import Boot._
    val downloader = new Downloader()
    val initialDelay = 1.minute

    logger.info(s"Open data address synchronization job will start in $initialDelay and will run " +
      s"every ${AddressConfig.updateRunInterval}")

    Source.tick(initialDelay, AddressConfig.updateRunInterval, Synchronize).runForeach { _ =>
      logger.info("Starting address open data synchronization job.")
      Future.sequence(List(
        downloader.download(url, directory, AddressFilePrefix, ".zip"),
        downloader.download(historyUrl, directory, AddressHistoryFilePrefix, ".zip"),
      )).map {
        case List(IOResult(ac, _), IOResult(hc, _)) =>
          if (ac >= 0) logger.info(s"Successfuly downloaded $ac bytes from $url")
          else logger.info(s"No bytes downloaded from $url")
          if (hc >= 0) logger.info(s"Successfuly downloaded $hc bytes from $historyUrl")
          else logger.info(s"No bytes downloaded from $historyUrl")
          if (ac > 0 || hc > 0) {
            logger.info(s"Deleting old address files...")
            val oldFiles =
              deleteOldFiles(directory, AddressFilePattern, AddressHistoryFilePattern)
            if (oldFiles.isEmpty) logger.info(s"No address files deleted.")
            else logger.info(s"Deleted address files - (${oldFiles.mkString(", ")})")
            publish(MsgEnvelope("check-new-version", CheckNewVersion))
          }
        case err => sys.error(s"Unexpected download result: $err")
      }.failed.foreach {
        logger.error(s"Error occured while downloading address from open data portal.", _)
      }
    }
  }

  class Downloader {
    implicit val system = ActorSystem("open-data-ar-download")
    implicit val ec     = system.dispatcher

    def download(url: String, destDir: String, prefix: String, suffix: String) = {

      def deriveAddressFile(resp: HttpResponse) = {
        val fn = resp.header[`Last-Modified`]
          .map(_.date.toIsoDateTimeString().replaceAll("[-:]", "_"))
          .map(prefix + _ + suffix)
          .getOrElse(sys.error(
            s"Last-Modified header was not found in response for '$url'. Cannot derive address data file name."))
        new File(destDir, fn)
      }

      Http().singleRequest(Head(url)).map(deriveAddressFile).collect { case f if f.exists =>
        logger.info(s"Head request info - address file already exists: $f")
        IOResult(-1)
      }.recoverWith { case _ =>
        Http().singleRequest(Get(url)).flatMap { resp =>
          val destFile = deriveAddressFile(resp)
          if (destFile.exists()) {
            logger.info(s"Address file already exists: $destFile")
            resp.discardEntityBytes()
            Future.successful(IOResult(-1))
          } else {
            logger.info(s"Downloading file: $destFile")
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
}
