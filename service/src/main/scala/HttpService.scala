package lv.addresses.service

import akka.actor.ActorSystem

import scala.language.postfixOps
import spray.json._
import akka.stream._
import akka.stream.scaladsl._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpResponse}
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.marshalling.{Marshaller, Marshalling, ToResponseMarshallable}
import akka.util.ByteString

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val f21 = jsonFormat21(AddressFull)
  implicit val f02 = jsonFormat2(ResolvedAddressFull)
  implicit val f14 = jsonFormat14(lv.addresses.indexer.AddressStruct)
}

case class AddressFull(
  code: Int, address: String, zipCode: Option[String], typ: Int,
  coordX: Option[BigDecimal], coordY: Option[BigDecimal], history: List[String],
  pilCode: Option[Int] = None, pilName: Option[String] = None,
  novCode: Option[Int] = None, novName: Option[String] = None,
  pagCode: Option[Int] = None, pagName: Option[String] = None,
  cieCode: Option[Int] = None, cieName: Option[String] = None,
  ielCode: Option[Int] = None, ielName: Option[String] = None,
  nltCode: Option[Int] = None, nltName: Option[String] = None,
  dzvCode: Option[Int] = None, dzvName: Option[String] = None)

 case class ResolvedAddressFull(address: String, resolvedAddress: Option[AddressFull])

import MyJsonProtocol._
import AddressService._

trait AddressHttpService extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport {

  val CODE_PATTERN = "(\\d{9,})"r

  val wsVersionNofifications =
    Flow.fromGraph(
      GraphDSL.create(
        Source.actorRef[Version](PartialFunction.empty, PartialFunction.empty, 0, OverflowStrategy.fail)
      .map { case v: Version => TextMessage.Strict(normalizeVersion(v.version))}) {
      import GraphDSL.Implicits._
      implicit builder => src =>
        val M = builder.add(Merge[Message](2))
        src ~> M
        FlowShape(M.in(1), M.out)
    }).mapMaterializedValue (actor => subscribe(actor, "version"))

  // Address to CSV marshaller
  implicit val addrAsCsv = Marshaller.strict[lv.addresses.indexer.Address, ByteString] { a =>
    Marshalling.WithFixedContentType(ContentTypes.`text/csv(UTF-8)`, () => {
      ByteString(List(a.code, a.address.replaceAll("[\n|;]",", "), a.typ, a.zipCode, a.coordX, a.coordY).mkString(";"))
    })
  }
  // enable csv streaming:
  implicit val csvStreaming = EntityStreamingSupport.csv()

  val route =
    (options & headerValueByType(Origin) &
      (path("address") | path("resolve") | path("address-structure"))) { origin =>
      respondWithHeaders(origin.origins.headOption.map { origin =>
        `Access-Control-Allow-Origin`(origin) } getOrElse (`Access-Control-Allow-Origin`.`*`),
        `Access-Control-Allow-Methods`(HttpMethods.GET)) {
          complete(HttpEntity.Empty)
        }
    } ~ handleWebSocketMessages(wsVersionNofifications) ~ path("") {
      redirect("/index.html", SeeOther)
    } ~ path("index.html") {
      getFromResource("index.html")
    } ~ (path("address") & get & parameterMultiMap) { params =>
      val pattern = params.get("search")
        .map(_.head match { case s if s.length > 256 => s take 256 case s => s })
        .getOrElse("")
      val limit = Math.min(params.get("limit") map (_.head.toInt) getOrElse 20, 100)
      val searchNearestLimit = params.get("limit") map (_.head.toInt) getOrElse 1
      val types = params.get("type") map(_.toSet.map((t: String) => t.toInt)) orNull
      val coordX: BigDecimal = params.get("x") map(x => BigDecimal(x.head)) getOrElse -1
      val coordY: BigDecimal = params.get("y") map(y => BigDecimal(y.head)) getOrElse -1
      respondWithHeader(`Access-Control-Allow-Origin`.`*`) {
        response {
          finder => (pattern match {
            case CODE_PATTERN(code) => finder.addressOption(code.toInt).toArray
            case p if coordX == -1 || coordY == -1 => finder.search(p)(limit, types)
            case _ => finder.searchNearest(coordX, coordY)(searchNearestLimit)
          }) map { a =>
            addrFull(a, finder.addressStruct(a.code), ", ").toJson
          }
        }
      }
    } ~ (path("resolve") & get & parameter("address")) { address =>
      respondWithHeader(`Access-Control-Allow-Origin`.`*`) { response {
        finder =>
          val ra = finder.resolve(address)
          ResolvedAddressFull(
            ra.address,
            ra.resolvedAddress.map(rao => addrFull(rao, finder.addressStruct(rao.code), "\n"))
          ).toJson
        }
      }
    } ~ (path("address-structure" / IntNumber) & get) { code =>
      respondWithHeader(`Access-Control-Allow-Origin`.`*`) {
        response(_.addressStruct(code).toJson)
      }
    } ~ path("version") {
      complete(finder.map(f => normalizeVersion(f.map(_.addressFileName).getOrElse(null))))
    } ~ (path("dump.csv") & get & parameterMultiMap) { params => {
        val types = params.get("type") map(_.toSet.map((t: String) => t.toInt))
        response {
          finder => finder.getAddressSource(types)
        }
      }
    } ~ pathSuffixTest(""".*(\.js|\.css|\.html|\.png|\.gif|\.jpg|\.jpeg|\.svg|\.woff|\.ttf|\.woff2)$"""r) { p => //static web resources TODO - make extensions configurable
      path(Remaining) { resource => getFromResource(resource) }
    }

    private def response(resp: AddressFinder => ToResponseMarshallable) = complete {
      finder.map(
        _.map(resp).getOrElse(
          ToResponseMarshallable(HttpResponse(ServiceUnavailable,
            entity = "Address service not initialized, try later, please."))
          )
        )
    }

    private def addrFull(
      a: lv.addresses.indexer.Address,
      struct: lv.addresses.indexer.AddressStruct,
      separator: String
    ) = {
      import struct._
      AddressFull(a.code, a.address.replace("\n", separator), Option(a.zipCode), a.typ,
        Option(a.coordX), Option(a.coordY), a.history,
        pilCode, pilName,
        novCode, novName,
        pagCode, pagName,
        cieCode, cieName,
        ielCode, ielName,
        nltCode, nltName,
        dzvCode, dzvName)
    }

    //beautification method
    private def normalizeVersion(version: String) =
      Option(version).map(_.split("""[/\\]""").last).getOrElse("<Not initialized>")
}

object Boot extends scala.App with AddressHttpService {

  val conf = com.typesafe.config.ConfigFactory.load

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("address-service")

  AddressService.publish(MsgEnvelope("check-new-version", CheckNewVersion))
  FTPDownload.initialize
  DbSync.initialize

  val bindingFuture = Http()
    .newServerAt("0.0.0.0",
      scala.util.Try(conf.getInt("address-service-port")).toOption.getOrElse(8082))
    .bind(route)
}
