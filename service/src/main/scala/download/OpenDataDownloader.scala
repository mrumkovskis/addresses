package lv.addresses.service.download

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.{Get, Head}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpEncodings.{compress, deflate, gzip}
import akka.http.scaladsl.model.headers.{HttpEncodings, `Accept-Encoding`, `Last-Modified`}
import akka.stream.IOResult
import akka.stream.scaladsl.FileIO
import com.typesafe.scalalogging.Logger

import java.io.File
import scala.concurrent.{ExecutionContext, Future}

case class OpenDataDownloadRes(srcUrl: String, tmpFile: File, destFile: File, ior: IOResult)

class OpenDataDownloader(logger: Logger)(implicit as: ActorSystem) {
  implicit val ec: ExecutionContext = as.dispatcher

  def download(url: String, destDir: String, fileNameBase: String): Future[OpenDataDownloadRes] = {

    def deriveFileFromLastModifiedHeader(resp: HttpResponse, discardBytes: Boolean) = {
      if (resp.status.isSuccess()) {
        val fn = resp.header[`Last-Modified`]
          .map(_.date.toIsoDateTimeString().replaceAll("[-:]", "_"))
          .map(_ + "." + fileNameBase)
          .getOrElse(sys.error(
            s"Last-Modified header was not found in response for '$url'. Cannot derive data file name."))
        val f = new File(destDir, fn)
        if (discardBytes || f.exists()) resp.discardEntityBytes()
        f
      } else {
        resp.discardEntityBytes()
        sys.error(s"HTTP error: ${resp.status}")
      }
    }

    Http().singleRequest(Head(url)).map(deriveFileFromLastModifiedHeader(_, true)).collect { case f if f.exists =>
      logger.info(s"Head request info - file already exists: $f")
      OpenDataDownloadRes(url, null, null, IOResult(-1))
    }.recoverWith { case _ =>
      import HttpEncodings._
      val req = Get(url).withHeaders(List(`Accept-Encoding`(gzip, compress, deflate)))
      Http().singleRequest(req).flatMap { resp =>
        val destFile = deriveFileFromLastModifiedHeader(resp, false)
        if (destFile.exists()) {
          logger.info(s"File already exists: $destFile")
          Future.successful(OpenDataDownloadRes(url, null, null, IOResult(-1)))
        } else {
          logger.info(s"Downloading file: $destFile")
          //store data first into temp file, so that loading process does not pick up partial file
          val tmpFile = File.createTempFile(fileNameBase, ".tmp", new File(destDir))
          resp.entity.dataBytes.runWith(FileIO.toPath(tmpFile.toPath))
            .map(OpenDataDownloadRes(url, tmpFile = tmpFile, destFile = destFile, _))
        }
      }
    }
  }
}
