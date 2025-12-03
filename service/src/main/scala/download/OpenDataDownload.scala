package lv.addresses.service.download

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.{Get, Head}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.{HttpEncodings, `Accept-Encoding`, `Last-Modified`}
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Source}
import com.typesafe.scalalogging.Logger
import lv.addresses.service.config.Configs
import lv.addresses.service.{AddressConfig, AddressService, Boot}
import org.slf4j.LoggerFactory

import java.io.File
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

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
      Future.sequence((urls ++ historyUrls).map(url => downloader.download(url, directory, fileNameFromUrl(url))))
        .map (_.map { case DownloadRes(url, tempFile, destFile, IOResult(count, _)) =>
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

  case class DownloadRes(srcUrl: String, tmpFile: File, destFile: File, ior: IOResult)

  class Downloader {
    implicit val system: ActorSystem  = ActorSystem("open-data-ar-download")
    implicit val ec: ExecutionContext = system.dispatcher

    def download(url: String, destDir: String, fileNameBase: String): Future[DownloadRes] = {

      def deriveAddressFile(resp: HttpResponse, discardBytes: Boolean) = {
        if (resp.status.isSuccess()) {
          val fn = resp.header[`Last-Modified`]
            .map(_.date.toIsoDateTimeString().replaceAll("[-:]", "_"))
            .map(_ + "." + fileNameBase)
            .getOrElse(sys.error(
              s"Last-Modified header was not found in response for '$url'. Cannot derive address data file name."))
          val f = new File(destDir, fn)
          if (discardBytes || f.exists()) resp.discardEntityBytes()
          f
        } else {
          resp.discardEntityBytes()
          sys.error(s"HTTP error: ${resp.status}")
        }
      }

      Http().singleRequest(Head(url)).map(deriveAddressFile(_, true)).collect { case f if f.exists =>
        logger.info(s"Head request info - address file already exists: $f")
        DownloadRes(url, null, null, IOResult(-1))
      }.recoverWith { case _ =>
        import HttpEncodings._
        val req = Get(url).withHeaders(List(`Accept-Encoding`(gzip, compress, deflate)))
        Http().singleRequest(req).flatMap { resp =>
          val destFile = deriveAddressFile(resp, false)
          if (destFile.exists()) {
            logger.info(s"Address file already exists: $destFile")
            Future.successful(DownloadRes(url, null, null, IOResult(-1)))
          } else {
            logger.info(s"Downloading file: $destFile")
            //store data first into temp file, so that loading process does not pick up partial file
            val tmpFile = File.createTempFile(fileNameBase, ".tmp", new File(destDir))
            resp.entity.dataBytes.runWith(FileIO.toPath(tmpFile.toPath))
              .map(DownloadRes(url, tmpFile = tmpFile, destFile = destFile, _))
          }
        }
      }
    }
  }
}
