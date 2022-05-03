package lv.addresses.service

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.pattern.ask
import akka.event.EventBus
import akka.event.LookupClassification

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import lv.addresses.indexer.{Addresses, IndexFiles}

import java.nio.file.{Files, Path}
import scala.util.matching.Regex

object AddressService extends EventBus with LookupClassification {

  sealed private[service] trait Msg
  private case object Finder extends Msg
  private case class Finder(af: AddressFinder) extends Msg
  private case class NewFinder(af: AddressFinder, version: String) extends Msg
  case object Version extends Msg
  case class Version(version: String) extends Msg
  private case class WatchVersionSubscriber(subscriber: Subscriber) extends Msg
  private[service] case object CheckNewVersion extends Msg
  private [service] case object Sync extends Msg

  case class MsgEnvelope(topic: String, payload: Msg)

  private[service] val as = ActorSystem("uniso-address-service")
  private[service] val addressFinderActor = as.actorOf(Props[AddressFinderActor](), "address-finder-actor")
  implicit val execCtx = as.dispatcher

  def finder = addressFinderActor.ask(Finder)(1.second).mapTo[Finder].map(f => Option(f.af))
  def version = addressFinderActor.ask(Version)(1.second).mapTo[Version].map(v => Option(v.version))

  //bus implementation
  type Event = MsgEnvelope
  type Classifier = String
  type Subscriber = ActorRef

  override def subscribe(subscriber: Subscriber, topic: Classifier) = {
    val res = super.subscribe(subscriber, topic)
    if (topic == "version") {
      addressFinderActor ! WatchVersionSubscriber(subscriber)
      as.log.info(s"$subscriber subscribed to version update notifications.")
    }
    res
  }

  // is used for extracting the classifier from the incoming events
  override protected def classify(event: Event): Classifier = event.topic

  // will be invoked for each event for all subscribers which registered themselves
  // for the event’s classifier
  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber ! event.payload
  }

  // must define a full order over the subscribers, expressed as expected from
  // `java.lang.Comparable.compare`
  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int =
    a.compareTo(b)

  // determines the initial size of the index data structure
  // used internally (i.e. the expected number of different classifiers)
  override protected def mapSize(): Int = 128
  //end of bus implementation

  class AddressFinderActor extends Actor {
    private var af: AddressFinder = null
    private var version: String = null
    override def receive = {
      case Finder => sender() ! Finder(af)
      case Version => sender() ! Version(version)
      case NewFinder(af, version) =>
        deleteOldIndexes()
        this.af = af
        this.version = version
        publish(MsgEnvelope("version", Version(version)))
      case WatchVersionSubscriber(subscriber) =>
        context.watch(subscriber)
        publish(MsgEnvelope("version", Version(version)))
      case Terminated(subscriber) =>
        unsubscribe(subscriber)
        as.log.info(s"$subscriber unsubscribed from version update.")
    }
    private def deleteOldIndexes() = {
      as.log.info(s"Deleting old index files...")
      import AddressConfig._
      val (ap, ip) = (addressConfig.AddressesPostfix, addressConfig.IndexPostfix)
      val oldFiles =
        deleteOldFiles(addressConfig.directory, s".+\\.$ap", s".+\\.$ip")
      if (oldFiles.isEmpty) as.log.info(s"No index files deleted.")
      else as.log.info(s"Deleted index files - (${oldFiles.mkString(", ")})")
    }

    override def postStop() = af = null
  }

  /** File age is determined by sorting file names. */
  def deleteOldFiles(dir: String, patterns: String*) = {
    patterns.map(new Regex(_)).flatMap { regex =>
      Files
        .list(Path.of(dir))
        .filter(p => regex.matches(p.getFileName.toString))
        .toArray
        .map(_.asInstanceOf[Path])
        .sortBy(_.getFileName)
        .dropRight(1) // keep newest file
        .map { p =>
          if (!p.toFile.delete()) as.log.warning(s"Unable to delete file: $p")
          p
        }.toList
    }
  }

  import Boot._
  import AddressConfig._
  //address updater job as stream
  Source
    .tick(updateRunInterval, updateRunInterval, CheckNewVersion) //periodical check
    .mergeMat(
      Source.actorRef(PartialFunction.empty, PartialFunction.empty,2, OverflowStrategy.dropHead))(
      (_, actor) => subscribe(actor, "check-new-version") //subscribe to check demand
    ).runFold(null: String) { (currentVersion, _) =>
      as.log.debug(s"Checking for address data new version, current version $currentVersion")
      val newVersion = addressConfig.version
      if (newVersion != null && (currentVersion == null || currentVersion < newVersion)) {
        as.log.info(s"New address data found. Initializing address finder $newVersion")
        val af = new AddressFinder(addressLoader, indexFiles)
        af.init
        addressFinderActor ! NewFinder(af, newVersion)
        newVersion
      } else {
        as.log.debug("No new address data found")
        currentVersion
      }
    }.onComplete {
      case Success(_) => as.log.info("Address updater job finished.")
      case Failure(err) => as.log.error(err, "Address updater terminated with failure.")
    }
}

class AddressFinder(val addressLoaderFun: () => Addresses, val indexFiles: IndexFiles)
extends lv.addresses.indexer.AddressFinder
//for debugging purposes
object AddressFinder extends lv.addresses.indexer.AddressFinder {
  def addressLoaderFun: () => Addresses = AddressConfig.addressLoader
  def indexFiles: IndexFiles = AddressConfig.indexFiles
}
