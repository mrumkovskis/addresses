package lv.addresses.service

import akka.stream.scaladsl.Source

import scala.concurrent.duration.DurationInt
import scala.util.Try

object DbSync {
  private case object Synchronize

  def initialize = {
    val config = com.typesafe.config.ConfigFactory.load

    import AddressService._

    if (config.hasPath("db")) { //do database synchronization only if 'db' conf parameter set
      import Boot._
      val initialDelay  = 1.minute
      as.log.info(s"Address databases synchronization job will start in $initialDelay and will run " +
        s"every $runInterval")
      Source.tick(initialDelay, runInterval, Synchronize).runForeach { _ =>
        as.log.info("Starting address databases synchronization job.")
        Try(lv.addresses.Updater.main(Array()))
          .map(_ => publish(MsgEnvelope("check-new-version", CheckNewVersion)))
          .failed.foreach { err =>
          as.log.error(err, "Error synchronizing address databases")
        }
      }
    } else
      as.log.info(s"Address databases synchronization job not started due to missing 'db' configuration")
  }
}
