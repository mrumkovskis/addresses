package lv.addresses.service

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http

import scala.util.Try
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.collection.JavaConversions._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorLogging
import akka.pattern.ask

import spray.can.server.Stats
import spray.can.server.UHttp
import spray.can.Http
import spray.routing._
import spray.util._
import spray.http._
import MediaTypes._
import spray.httpx.encoding.Gzip
import spray.httpx.marshalling.Marshaller
import spray.json._
import spray.http.HttpHeaders._
import spray.can.websocket._
import spray.can.websocket.frame.{ Frame, FrameStream, BinaryFrame, TextFrame }

import com.typesafe.config._

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val f01 = jsonFormat18(AddressFull)
  implicit val f14 = jsonFormat14(lv.addresses.indexer.AddressStruct)
}

case class AddressFull(code: Int, address: String, zipCode: Option[String], typ: Int,
  pilCode: Option[Int] = None, pilName: Option[String] = None,
  novCode: Option[Int] = None, novName: Option[String] = None,
  pagCode: Option[Int] = None, pagName: Option[String] = None,
  cieCode: Option[Int] = None, cieName: Option[String] = None,
  ielCode: Option[Int] = None, ielName: Option[String] = None,
  nltCode: Option[Int] = None, nltName: Option[String] = None,
  dzvCode: Option[Int] = None, dzvName: Option[String] = None)

import MyJsonProtocol._
import AddressService._

class WsServiceActor(val serverConnection: ActorRef,
  val businessLogic: PartialFunction[Any, Any]) extends Actor with ActorLogging {

    println(s"\n\nCreated ws actor with conn: $serverConnection\n\n")

  override def receive = akka.event.LoggingReceive { handshaking orElse closeLogic }

  def work: Receive = {
    // just bounce frames back for the time being
    case x @ (_: BinaryFrame | _: TextFrame) =>
      sender ! x

    //is defined is checked in businessLogic, so accept any string message
    case businessMessage: String => send(TextFrame(businessMessage))

    case x: FrameCommandFailed =>
      log.error("frame command failed", x)

    case x: HttpRequest => // do something
  }

  def handshaking: Receive = {
    // when a client request for upgrading to websocket comes in, we send
    // UHttp.Upgrade to upgrade to websocket pipelines with an accepting response.
    case wsFailure: HandshakeFailure => serverConnection ! wsFailure.response
    case wsContext: HandshakeContext => serverConnection ! UHttp.UpgradeServer(
      pipelineStage(self, wsContext), wsContext.response)

    // upgraded successfully
    case UHttp.Upgraded =>
      context.become(
        akka.event.LoggingReceive { (businessLogic andThen work) orElse closeLogic })
      subscribe(self, "version")
  }

  def closeLogic: Receive = {
    case ev: Http.ConnectionClosed =>
      context.stop(self)
      log.debug("Connection closed on event: {}", ev)
  }

  def send(frame: Frame) {
    serverConnection ! FrameCommand(frame)
  }

  def send(frame: FrameStream) {
    serverConnection ! FrameStreamCommand(frame)
  }

  override def postStop() = unsubscribe(self)

}

trait AddressHttpService extends HttpService {

  val CODE_PATTERN = "(\\d{9,})"r

  val wsLogic: PartialFunction[Any, Any] = {
    case Version(version) => normalizeVersion(version)
  }

  val route = detach() {
    dynamic {
      path("") {
        redirect("/index.html", StatusCodes.SeeOther)
      } ~ path("index.html") {
        getFromResource("index.html")
      } ~ (path("address") & get & parameterMultiMap) { params =>
        val pattern = params.get("search") map (_.head) getOrElse ("")
        val limit = params.get("limit") map (_.head.toInt) getOrElse 20
        val types = params.get("type").map(_.toSet.map((t: String) => t.toInt)).orNull
        complete(
          (for {
            f <- finder
            s <- (pattern match {
              case CODE_PATTERN(code) => address(code.toInt) map (_.toArray)
              case p => search(p, limit, types)
            })
          } yield {
            s map { a =>
              val st = f.addressStruct(a.code)
              import st._
              AddressFull(a.code, a.address, Option(a.zipCode), a.typ,
                pilCode, pilName,
                novCode, novName,
                pagCode, pagName,
                cieCode, cieName,
                ielCode, ielName,
                nltCode, nltName,
                dzvCode, dzvName)
            }
          }) map { _.toJson.prettyPrint })
      } ~ (path("address-structure" / IntNumber)) { code =>
        complete(struct(code) map (_.toJson.prettyPrint))
      } ~ path("version") {
        complete(version map (normalizeVersion(_)))
      } ~ pathSuffixTest(""".*(\.js|\.css|\.html|\.png|\.gif|\.jpg|\.jpeg|\.svg|\.woff|\.ttf|\.woff2)$"""r) { p => //static web resources TODO - make extensions configurable
        path(Rest) { resource => getFromResource(resource) }
      }
    }
  }

}

class AddressHttpServer extends HttpServiceActor with AddressHttpService with ActorLogging {
  def wsHandshaking: Receive = {
    case HandshakeRequest(state) =>
      val serverConnection = sender()
      val conn = context.actorOf(Props(classOf[WsServiceActor],
        serverConnection, wsLogic))
      conn ! state
  }

  def receive: Receive = akka.event.LoggingReceive { wsHandshaking orElse runRoute(route) }

}

object Boot extends scala.App {

  val conf = com.typesafe.config.ConfigFactory.load

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("address-service")

  // create and start webhouse service actor
  val service = system.actorOf(Props[AddressHttpServer], "address-service")

  AddressService.maybeInit

  // start a new HTTP server with webhouse service actor as the handler and web socket support
  IO(UHttp) ! Http.Bind(service, interface = "0.0.0.0",
    port = scala.util.Try(conf.getInt("address-service-port")).toOption.getOrElse(8082))
}
