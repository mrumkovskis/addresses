package lv.addresses.service

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Status
import akka.actor.Stash
import akka.actor.Terminated
import akka.pattern.ask
import akka.event.EventBus
import akka.event.LookupClassification
import scala.concurrent.duration._
import scala.language.postfixOps

object AddressService extends AddressServiceConfig with EventBus with LookupClassification {

  private trait Msg
  private case object Finder extends Msg
  private case class Finder(af: AddressFinder) extends Msg
  private case object CheckNewVersion extends Msg

  private case class Subscribe(subscriber: Subscriber)
  case class Version(version: String)
  case class MsgEnvelope(topic: String, payload: Version)

  private[service] val as = ActorSystem("uniso-address-service")
  private[service] val finderActor = as.actorOf(Props[AddressFinderActor], "address-finder-actor")
  private val updaterActor = as.actorOf(Props(classOf[AddressUpdaterActor], finderActor), "address-updater-actor")
  implicit val execCtx = as.dispatcher

  def finder = finderActor.ask(Finder)(1.second).mapTo[Finder].map(f => Option(f.af))
  private[service] def checkNewVersion = updaterActor ! CheckNewVersion

  //bus implementation
  type Event = MsgEnvelope
  type Classifier = String
  type Subscriber = ActorRef

  override def subscribe(subscriber: Subscriber, topic: Classifier) = {
    val res = super.subscribe(subscriber, topic)
    updaterActor ! Subscribe(subscriber)
    as.log.info(s"$subscriber subscribed to version update.")
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
  override protected def mapSize: Int = 128
  //end of bus implementation

  class AddressFinderActor extends Actor {
    private var af: AddressFinder = null
    override def receive = {
      case Finder => sender ! Finder(af)
      case Finder(af) =>
        deleteOldIndexes
        this.af = af
    }
    private def deleteOldIndexes = if (af != null) {
      val (cache, index) = (af.addressCacheFile(af.addressFileName),
        af.indexFile(af.addressFileName))
      if (cache.delete) as.log.info(s"Deleted address cache: $cache") else
        as.log.warning(s"Unable to delete address cache file $cache")
      if (index.delete) as.log.info(s"Deleted address index: $index") else
        as.log.warning(s"Unable to delete address index: $index")
    }
    override def postStop = af = null
  }

  class AddressUpdaterActor(addressFinderActor: ActorRef) extends Actor {
    private val checkScheduler = context.system.scheduler.schedule(
      initializerRunInterval, initializerRunInterval, self, CheckNewVersion)
    var currentVersion: String = null
    private def mayBeUpdateFinder = {
      val newVersion = addressFileName
      if (newVersion != null && (currentVersion == null || currentVersion < newVersion)) {
        val af = new AddressFinder(newVersion, blackList, houseCoordFile)
        af.init
        currentVersion = newVersion
        addressFinderActor ! Finder(af)
        publish(MsgEnvelope("version", Version(currentVersion)))
      }
    }
    override def preStart = mayBeUpdateFinder
    override def receive = {
      case CheckNewVersion => mayBeUpdateFinder
      case Subscribe(subscriber) =>
        context.watch(subscriber)
        publish(MsgEnvelope("version", Version(currentVersion)))
      case Terminated(subscriber) =>
        unsubscribe(subscriber)
        as.log.info(s"$subscriber unsubscribed from version update.")
    }
    override def postStop = checkScheduler.cancel
  }
}

trait AddressServiceConfig extends lv.addresses.indexer.AddressIndexerConfig {
  def conf = com.typesafe.config.ConfigFactory.load
  def akFileName = if (conf.hasPath("VZD.ak-file")) conf.getString("VZD.ak-file") else {
    println("ERROR: address file setting 'VZD.ak-file' not found")
    null
  }
  def akDirName = {
    val idx = akFileName.lastIndexOf('/')
    if (idx != -1) akFileName.substring(0, idx) else "."
  }
  def akFileNamePattern = akFileName.substring(akFileName.lastIndexOf('/') + 1)
  def blackList: Set[String] = if (conf.hasPath("VZD.blacklist"))
    conf.getString("VZD.blacklist").split(",\\s+").toSet else Set()
  val initOnStartup =
    if (conf.hasPath("VZD.init-on-startup")) conf.getBoolean("VZD.init-on-startup") else false
  val workerActorCount =
    if (conf.hasPath("VZD.worker-actor-count")) conf.getInt("VZD.worker-actor-count") else 5
  import scala.concurrent.duration._
  private val dur = if (conf.hasPath("VZD.initializer-run-interval"))
    Duration(conf.getString("VZD.initializer-run-interval")) else 1 hour
  val initializerRunInterval = FiniteDuration(dur.length, dur.unit)

  //return alphabetically last file name matching pattern
  def addressFileName: String = {
    Option(new java.io.File(akDirName)
      .listFiles(new java.io.FileFilter {
        def accept(f: java.io.File) =
          java.util.regex.Pattern.matches(akFileNamePattern, f.getName)
      })).flatMap {
        _.sortBy(_.getName)
          .lastOption
          .map(_.getPath)
      }.orNull
  }
  def houseCoordFile = scala.util.Try(conf.getString("VZD.house-coord-file")).toOption.orNull
}

class AddressFinder(val addressFileName: String, val blackList: Set[String],
  val houseCoordFile: String)
extends lv.addresses.indexer.AddressFinder
//for debugging purposes
object AddressFinder extends lv.addresses.indexer.AddressFinder with AddressServiceConfig
