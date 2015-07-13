package lv.addresses.service

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http

import scala.util.Try
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.collection.JavaConversions._

import akka.actor.Actor
import akka.pattern.ask

import spray.can.server.Stats
import spray.can.Http
import spray.routing._
import spray.util._
import spray.http._
import MediaTypes._
import spray.httpx.encoding.Gzip
import spray.httpx.marshalling.Marshaller
import spray.json._
import spray.http.HttpHeaders._

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

class AddressServiceActor extends Actor with AddressHttpService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = akka.event.LoggingReceive { runRoute(route) }
}

trait AddressHttpService extends HttpService {

  val CODE_PATTERN = "(\\d{9,})"r

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
        complete(version map (Option(_).map(_.split("""[/\\]""").last).getOrElse("<Not initialized>")))
      } ~ pathSuffixTest(""".*(\.js|\.css|\.html|\.png|\.gif|\.jpg|\.jpeg|\.svg|\.woff|\.ttf|\.woff2)$"""r) { p => //static web resources TODO - make extensions configurable
        path(Rest) { resource => getFromResource(resource) }
      }
    }
  }
}

object Boot extends scala.App {

  val conf = com.typesafe.config.ConfigFactory.load

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("address-service")

  // create and start webhouse service actor
  val service = system.actorOf(Props[AddressServiceActor], "address-service")

  AddressService.maybeInit

  // start a new HTTP server with webhouse service actor as the handler
  IO(Http) ! Http.Bind(service, interface = "0.0.0.0",
    port = scala.util.Try(conf.getInt("address-service-port")).toOption.getOrElse(8082))
}

