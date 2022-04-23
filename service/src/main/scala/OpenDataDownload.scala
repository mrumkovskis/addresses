package lv.addresses.service

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.headers.`Last-Modified`
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Source}

import java.nio.file.Path
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object OpenDataDownload {

  private case object Synchronize

  def initialize = {
    val config = com.typesafe.config.ConfigFactory.load

    import AddressService._

    if (config.hasPath("VZD.open-data")) {
      val conf = config.getConfig("VZD.open-data")
      import Boot._
      val url = conf.getString("url")
      val historyUrl = conf.getString("history-url")
      val saveToDir = conf.getString("save-to-dir")
      val downloader = new Downloader()
      val initialDelay = 1.minute

      as.log.info(s"Open data address synchronization job will start in $initialDelay and will run " +
        s"every $runInterval")

      Source.tick(initialDelay, runInterval, Synchronize).runForeach { _ =>
        as.log.info("Starting address open data synchronization job.")
        downloader.download(url, saveToDir, "VAR_", ".zip").flatMap {
          case IOResult(count, Success(_)) =>
            system.log.debug(s"Successfuly downloaded $count bytes from $url")
            downloader.download(historyUrl, saveToDir, "VAR_HIS_", ".zip").flatMap {
              case IOResult(count, Success(_)) =>
                system.log.debug(s"Successfuly downloaded $count bytes from $historyUrl")
                publish(MsgEnvelope("check-new-version", CheckNewVersion))
                Future.successful(Done)
              case IOResult(_, Failure(exception)) => Future.failed(exception)
            }
          case IOResult(_, Failure(exception)) => Future.failed(exception)
        }.failed.foreach {
          as.log.error(_, s"Error occured while downloading address from open data portal.")
        }
      }

    } else {
      as.log.info("Address open data synchronization job not started due to missing 'VZD.open-data' configuration")
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
        resp.entity.dataBytes.runWith(FileIO.toPath(Path.of(destDir, fileName)))
      }
    }
  }
}
