package lv.addresses.service.download

import akka.stream.scaladsl.Source
import lv.addresses.service.{AddressService, Boot}

import scala.concurrent.duration.DurationInt
import scala.util.Try

object DbSync {
  private case object Synchronize

  def initialize = {
    val config = com.typesafe.config.ConfigFactory.load

    import AddressService._

    import Boot._
    import lv.addresses.service.AddressConfig._
    val initialDelay  = 1.minute
    as.log.info(s"Address databases synchronization job will start in $initialDelay and will run " +
      s"every $updateRunInterval")
    Source.tick(initialDelay, updateRunInterval, Synchronize).runForeach { _ =>
      as.log.info("Starting address databases synchronization job.")
      Try(lv.addresses.Updater.main(Array()))
        .map(_ => publish(MsgEnvelope("check-new-version", CheckNewVersion)))
        .failed.foreach { err =>
        as.log.error(err, "Error synchronizing address databases")
      }
    }
  }
}
