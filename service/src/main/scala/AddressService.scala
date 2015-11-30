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
  private case class Search(pattern: String, limit: Int, types: Set[Int], af: AddressFinder = null) extends Msg
  private case class Struct(code: Int, af: AddressFinder = null) extends Msg
  private case class Address(code: Int, af: AddressFinder = null) extends Msg
  private case class Init(akFileName: String, blackList: Set[String], houseCoordFile: String) extends Msg
  private case class Ready(serverActor: ActorRef) extends Msg
  private case object Initialize extends Msg
  private case object Finder extends Msg
  private case object Shutdown extends Msg
  private case object CleanIndexes extends Msg
  private case object GetVersion extends Msg
  private case class Subscribe(subscriber: Subscriber)

  case class Version(version: String)
  case class MsgEnvelope(topic: String, payload: Any)

  private[service] val as = ActorSystem("uniso-address-service")
  private val proxy = as.actorOf(Props[Proxy])
  private val initializer = as.actorOf(Props[Initializer])
  implicit val execCtx = as.dispatcher

  def maybeInit = if (initOnStartup) initializer ! Initialize

  def search(pattern: String, limit: Int, types: Set[Int]) = proxy
    .ask(Search(pattern, limit, types))(30.second).mapTo[Array[lv.addresses.indexer.Address]]
  def struct(code: Int) = proxy
    .ask(Struct(code))(30.second).mapTo[lv.addresses.indexer.AddressStruct]
  def address(code: Int) = proxy
    .ask(Address(code))(30.second).mapTo[Option[lv.addresses.indexer.Address]]
  def finder = proxy.ask(Finder)(30.second).mapTo[AddressFinder]
  def shutdown = proxy.ask(Shutdown)(30.second)
  def version = proxy.ask(GetVersion)(30.second).mapTo[Version].map(_.version)
  def initialize = initializer.ask(Initialize)(30.second)

  //bus implementation
  type Event = MsgEnvelope
  type Classifier = String
  type Subscriber = ActorRef

  override def subscribe(subscriber: Subscriber, topic: Classifier) = {
    val res = super.subscribe(subscriber, topic)
    initializer ! Subscribe(subscriber)
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

  //beautification method
  def normalizeVersion(version: String) =
    Option(version).map(_.split("""[/\\]""").last).getOrElse("<Not initialized>")

  class Proxy extends Actor with Stash {

    private var server: ActorRef = null

    def receive = notInitialized

    def notInitialized: Receive = {
      case r @ Ready(server) =>
        context.become(serve)
        this.server = server
        initializer forward r
        unstashAll
        //get version for publishing
        server ! GetVersion
      case GetVersion => sender ! Version(null)
      case _: Msg =>
        initializer ! Initialize
        stash
    }

    def serve: Receive = {
      case s: Search => server forward s
      case s: Struct => server forward s
      case a: Address => server forward a
      case Finder => server forward Finder
      case GetVersion => server forward GetVersion
      case v: Version => publish(MsgEnvelope("version", v))
      case r @ Ready(server) =>
        this.server ! CleanIndexes
        this.server ! Shutdown
        this.server = server
        initializer forward r
        //get version for publishing
        server ! GetVersion
      case Shutdown =>
        server ! Shutdown
        server = null
        sender ! "Server shutdown signal sent..."
    }
  }

  class Initializer extends Actor {

    var ready = true

    //start initializer scheduler
    private val initScheduler =
      context.system.scheduler.schedule(initializerRunInterval, initializerRunInterval, self, Initialize)

    def receive = {
      case Initialize =>
        if (ready) proxy ! GetVersion
        else as.log.info("Initializer not started, another initialization in progress!")
      case Version(version) if ready =>
        val fn = addressFileName
        if (fn != null && (version == null || version < fn)) {
          context.actorOf(Props[Server]).tell(Init(fn, blackList,
            houseCoordFile), proxy)
          ready = false
        } else {
          if (fn == null && version == null) proxy ! Status.Failure(new RuntimeException(
            s"Unable to load addresses using address file pattern $akFileName!"))
          as.log.debug(s"Not initializing address file '$fn'. Current version - '$version'")
        }
      case Ready(server) => ready = true
      case Subscribe(subscriber) => context.watch(subscriber)
      case Terminated(subscriber) =>
        unsubscribe(subscriber)
        as.log.info(s"$subscriber unsubscribed from version update.")
    }

    override def postStop() = initScheduler.cancel

  }

  class Server extends Actor {

    import akka.routing.ActorRefRoutee
    import akka.routing.Router
    import akka.routing.RoundRobinRoutingLogic

    var routerNr = 1
    private def createRoutee = {
      val r = context.actorOf(Props[Worker], s"address_router_$routerNr")
      routerNr += 1
      context watch r
      ActorRefRoutee(r)
    }

    private var router = {
      as.log.info(s"Initializing $workerActorCount workers")
      val routees = Vector.fill(workerActorCount) {
        createRoutee
      }
      Router(RoundRobinRoutingLogic(), routees)
    }

    private var af: AddressFinder = null

    def receive = init

    def init: Receive = {
      case Init(akFileName, blackList, houseCoordFile) =>
        af = new AddressFinder(akFileName, blackList, houseCoordFile)
        af.init
        context.become(serve, true)
        sender ! Ready(self)
    }

    def serve: Receive = {
      case s: Search => router.route(s.copy(af = af), sender)
      case s: Struct => router.route(s.copy(af = af), sender)
      case a: Address => router.route(a.copy(af = af), sender)
      case Finder => sender ! af
      case GetVersion => sender ! Version(af.addressFileName)
      case Terminated(a) =>
        router = router.removeRoutee(a)
        createRoutee
        as.log.info(s"Terminated routee: $a")
      case CleanIndexes =>
        val (cache, index) = (af.addressCacheFile(af.addressFileName),
          af.indexFile(af.addressFileName))
        if (cache.delete) as.log.info(s"Deleted address cache: $cache") else
          as.log.warning(s"Unable to delete address cache file $cache")
        if (index.delete) as.log.info(s"Deleted address index: $index") else
          as.log.warning(s"Unable to delete address index: $index")
      case Shutdown =>
        router.routees.foreach {
          case ActorRefRoutee(ref) =>
            context unwatch ref
            context.stop(ref)
        }
        context.stop(self)
    }

    override def postStop() {
      as.log.info(s"Server stopped: ${if (af != null) af.addressFileName else ""}")
      af = null
    }

  }

  class Worker extends Actor {
    def receive = {
      case Search(pattern, limit, types, af) => process(af.search(pattern, limit, types))
      case Struct(code, af) => process(af.addressStruct(code))
      case Address(code, af) => process(af.addressOption(code))
    }
    def process(block: => Any) = try {
      val result = block
      sender ! result
    } catch {
      case e: Exception => sender ! Status.Failure(new RuntimeException("Error occured during address search!", e))
    }
    override def postStop() {
      as.log.info(s"Stopping address router $self")
    }
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
