package lv.addresses.service

import java.io.{File, FileInputStream}
import java.security.{KeyStore, SecureRandom}
import akka.actor.ActorSystem

import scala.language.postfixOps
import spray.json._
import akka.stream._
import akka.stream.scaladsl._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpResponse, MediaTypes}
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.marshalling.{Marshaller, Marshalling, ToResponseMarshallable}
import akka.http.scaladsl.server.Directive1
import akka.util.ByteString
import download.{DbSync, FTPDownload, OpenDataDownload}
import lv.addresses.indexer.AddressFields._
import lv.addresses.indexer.{MutableAddress, ResolvedAddress}
import lv.addresses.service.config.Configs

import java.time.LocalDate
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import scala.collection.mutable.{ArrayBuffer => AB}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

object MyJsonProtocol extends DefaultJsonProtocol {
  def mutableAddressJsonizer(obj: MutableAddress): JsValue = {
    val res = new AB[(String, JsValue)](16)
    res += ("code" -> JsNumber(obj.code))
    res += ("typ" -> JsNumber(obj.typ))
    res += ("address" -> JsString(obj.address))
    res += ("irAdrese" -> JsBoolean(obj.irAdrese))
    if (obj.zipCode != null) res += ("zipCode" -> JsString(obj.zipCode))
    if (obj.lksCoordX != null) res += ("lksCoordX" -> JsNumber(obj.lksCoordX))
    if (obj.lksCoordY != null) res += ("lksCoordY" -> JsNumber(obj.lksCoordY))
    if (obj.history != null) res += ("history" -> JsArray(obj.history.map(JsString(_)).toVector))
    obj.pilCode.foreach(v => res += ("pilCode" -> JsNumber(v)))
    obj.pilName.foreach(v => res += ("pilName" -> JsString(v)))
    obj.novCode.foreach(v => res += ("novCode" -> JsNumber(v)))
    obj.novName.foreach(v => res += ("novName" -> JsString(v)))
    obj.pagCode.foreach(v => res += ("pagCode" -> JsNumber(v)))
    obj.pagName.foreach(v => res += ("pagName" -> JsString(v)))
    obj.cieCode.foreach(v => res += ("cieCode" -> JsNumber(v)))
    obj.cieName.foreach(v => res += ("cieName" -> JsString(v)))
    obj.ielCode.foreach(v => res += ("ielCode" -> JsNumber(v)))
    obj.ielName.foreach(v => res += ("ielName" -> JsString(v)))
    obj.nltCode.foreach(v => res += ("nltCode" -> JsNumber(v)))
    obj.nltName.foreach(v => res += ("nltName" -> JsString(v)))
    obj.dzvCode.foreach(v => res += ("dzvCode" -> JsNumber(v)))
    obj.dzvName.foreach(v => res += ("dzvName" -> JsString(v)))
    obj.pilAtvk.foreach(v => res += ("pilAtvk" -> JsString(v)))
    obj.novAtvk.foreach(v => res += ("novAtvk" -> JsString(v)))
    obj.pagAtvk.foreach(v => res += ("pagAtvk" -> JsString(v)))
    obj.editDistance.foreach(ed => res += ("editDistance" -> JsNumber(ed)))
    JsObject(res.toMap)
  }

  def resolvedAddressJsonizer(obj: ResolvedAddress): JsObject = {
    val res = new AB[(String, JsValue)](2)
    res += ("address" -> JsString(obj.address))
    obj.resolvedAddress.foreach(ra => res += ("resolvedAddress" -> mutableAddressJsonizer(ra)))
    JsObject(res.toMap)
  }
}

import MyJsonProtocol._
import AddressService._

trait AddressHttpService extends lv.addresses.service.Authorization with
  akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport {

  val conf = com.typesafe.config.ConfigFactory.load

  val authKeys: Set[String] =
    if(conf.hasPath("auth.keys"))
      conf.getList("auth.keys").unwrapped.asScala.map(String.valueOf).toSet
    else Set()

  val CODE_PATTERN = "(\\d{9})"r

  val Min_LKS_X = 300000
  val Max_LKS_X = 770000
  val Min_LKS_Y = 160000
  val Max_LKS_Y = 450000

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
  implicit val addrAsCsv = Marshaller.strict[lv.addresses.indexer.MutableAddress, ByteString] { a =>
    Marshalling.WithFixedContentType(ContentTypes.`text/csv(UTF-8)`, () => {
      ByteString(List(a.code, a.address.replaceAll("[\n|;]",", "), a.typ, a.zipCode, a.lksCoordX, a.lksCoordY).mkString(";"))
    })
  }
  // enable csv streaming:
  implicit val csvStreaming = EntityStreamingSupport.csv()

  val route =
    (options & headerValueByType(Origin) &
      (path("address") | path("resolve") | path("address-struct"))) { origin =>
      respondWithHeaders(origin.origins.headOption.map { origin =>
        `Access-Control-Allow-Origin`(origin) } getOrElse (`Access-Control-Allow-Origin`.`*`),
        `Access-Control-Allow-Methods`(HttpMethods.GET)) {
          complete(HttpEntity.Empty)
        }
    } ~ handleWebSocketMessages(wsVersionNofifications) ~ pathEndOrSingleSlash {
      redirect("index.html", SeeOther)
    } ~ path("index.html") {
      getFromResource("index.html")
    } ~ authenticate {
      (path("address") & get & parameterMultiMap) { params =>
        addressRoute(Set(), params)
      } ~ (path("address-struct") & get & parameterMultiMap) { params =>
        addressRoute(Set(StructData, LksKoordData, HistoryData, AtvkData), params)
      } ~ (path("resolve") & get & parameter("address")) { address =>
        respondWithHeader(`Access-Control-Allow-Origin`.`*`) {
          response { finder =>
            resolvedAddressJsonizer(finder.resolve(address))
          }
        }
      } ~ path("openapi.yaml") {
        getFromResource("openapi.yaml", ContentTypes.`text/plain(UTF-8)`)
      } ~ path("version") {
        complete(finder.map(f => normalizeVersion(f.map(_.addressFileName).getOrElse(null))))
      } ~ (path("dump.csv") & get & parameterMultiMap) {
        params => {
          val types = params.get("type") map(_.toSet.map((t: String) => t.toInt))
          response {
            finder => finder.getAddressSource(types)
          }
        }
      } ~ path("adreses.jar") {
        Try(conf.getString("address-artifact-file-name"))
          .map(new File(_))
          .filter(_.exists)
          .map { file =>
            respondWithHeader(
              `Content-Disposition`(
                ContentDispositionTypes.attachment, Map("filename" -> file.getName))) {
              getFromFile(file, MediaTypes.`application/java-archive`)
            }
          }
          .getOrElse(complete(NotFound))
      }
    } ~ reloadBlockedUsers ~ pathSuffixTest(
    """.*(\.js|\.css|\.html|\.png|\.gif|\.jpg|\.jpeg|\.svg|\.woff|\.ttf|\.woff2)$"""r) { p => //static web resources TODO - make extensions configurable
      path(Remaining) { resource => getFromResource(resource) }
    }

  val service =
    if (authKeys.isEmpty) route
    else {
      pathPrefix(Segment)
        .filter(authKeys.contains)
        .recover(_ => complete(NotFound, "404 Not Found"): Directive1[String])
        .apply { key => //somehow apply call needed?
          pathEnd { redirect(s"$key/", SeeOther) } ~ route
        }
    }

  private def addressRoute(fields: Set[String], params: Map[String, List[String]]) = {
    val pattern = params.get("search")
      .map(_.head match { case s if s.length > 256 => s take 256 case s => s })
      .getOrElse("")
    val limit = Try {
      params.get("limit")
        .flatMap (_.headOption)
        .filter(_.nonEmpty)
        .map(_.toInt)
        .map(Math.min(_, 100))
        .getOrElse(20)
    }
    val searchNearestLimit = Try {
      params.get("limit")
        .flatMap (_.headOption)
        .filter(_.nonEmpty)
        .map(_.toInt)
        .map(Math.min(_, 100))
        .getOrElse(1)
    }
    val types = Try {
      params.get("type")
        .filter(_.nonEmpty)
        .filter(_.forall(_.nonEmpty))
        .map(_.map(_.toInt).toSet)
    }
    val coordX = Try {
      params.get("lks_x")
        .flatMap(_.headOption)
        .filter(_.nonEmpty)
        .map(BigDecimal(_))
    }
    val coordY = Try {
      params.get("lks_y")
        .flatMap(_.headOption)
        .filter(_.nonEmpty)
        .map(BigDecimal(_))
    }
    val aFields = {
      val ms = scala.collection.mutable.Set[String]()
      params.get(StructData).foreach(_ => ms += StructData)
      params.get(LksKoordData).foreach(_ => ms += LksKoordData)
      params.get(AtvkData).foreach(_ => ms += AtvkData)
      params.get(HistoryData).foreach(_ => ms += HistoryData)
      fields ++ ms
    }

    respondWithHeader(`Access-Control-Allow-Origin`.`*`) {
      response {
        finder =>
          val err = new StringBuilder
          def regErr(x: Try[_], msg: String) = if (x.isFailure) err.append(msg)
          regErr(limit, "'limit' parameter must be number\n")
          regErr(types, "'type' parameter(s) must be number(s)\n")
          regErr(coordX, "'lks_x' parameter must be decimal number\n")
          regErr(coordY, "'lks_y' parameter must be decimal number\n")
          if (err.nonEmpty) {
            ToResponseMarshallable(HttpResponse(BadRequest, entity = err.toString))
          } else {
            def validateCoorBox = coordX.get zip coordY.get exists {
              case (x, y) => x >= Min_LKS_X && x <= Max_LKS_X && y >= Min_LKS_Y && y <= Max_LKS_Y
            }
            (pattern match {
              case CODE_PATTERN(code) => finder.mutableAddressOption(code.toInt, aFields).toArray
              case p if coordX.get.isEmpty || coordY.get.isEmpty => finder.search(p)(limit.get, types.get.orNull, aFields)
              case _ if validateCoorBox =>
                finder.searchNearest(coordX.get.get, coordY.get.get)(searchNearestLimit.get, aFields)
              case _ => Array[MutableAddress]() // return empty array coords are out of box
            }) map { a =>
              a.address = a.address.replace("\n", ", ")
              mutableAddressJsonizer(a)
            }
          }
      }
    }
  }

  private def response(resp: AddressFinder => ToResponseMarshallable) = complete {
      finder.map(
        _.map(resp).getOrElse(
          ToResponseMarshallable(HttpResponse(ServiceUnavailable,
            entity = "Address service not initialized, try later, please."))
          )
        )
    }

  //beautification method
  private def normalizeVersion(version: String) =
    Option(version).map(_.split("""[/\\]""").last).getOrElse("<Not initialized>")
}

object Boot extends scala.App with AddressHttpService {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("address-service")

  AddressService.publish(MsgEnvelope("check-new-version", CheckNewVersion))

  system.log.info(s"Configuration: ${AddressConfig.addressConfig}")

  // start sync process
  AddressConfig.addressConfig match {
    case _: Configs.Db        => DbSync.initialize
    case c: Configs.OpenData  => OpenDataDownload.initialize(c)
    case c: Configs.AK        => FTPDownload.initialize(c.directory, c.akFileNamePattern)
  }

  val bindingFuture =
    if (conf.hasPath("ssl")) {
      val sslConf = conf.getConfig("ssl")
      def c(p: String) = sslConf.getString(p)
      val ksf = c("key-store")
      val ksp = c("key-store-password")
      val kst = c("key-store-type")
      val ks = KeyStore.getInstance(kst)
      ks.load(new FileInputStream(ksf), ksp.toCharArray)
      val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
      keyManagerFactory.init(ks, ksp.toCharArray)

      val tsf = c("trust-store")
      val tsp = c("trust-store-password")
      val tst = c("trust-store-type")
      val ts = KeyStore.getInstance(tst)
      ts.load(new FileInputStream(tsf), tsp.toCharArray)
      val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
      tmf.init(ts)

      val sslContext: SSLContext = SSLContext.getInstance("TLS")
      sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)

      val clientAuth = Try(sslConf.getBoolean("client-auth")).toOption.getOrElse(false)
      val https: HttpsConnectionContext =
        if (clientAuth) {
          ConnectionContext.httpsServer(() => {
            val engine = sslContext.createSSLEngine()
            engine.setUseClientMode(false)
            engine.setNeedClientAuth(true)
            engine
          })
        } else {
          ConnectionContext.httpsServer(sslContext)
        }
      Http()
        .newServerAt("0.0.0.0", Try(sslConf.getInt("port")).toOption.getOrElse(443))
        .enableHttps(https)
        .bind(service)
    } else {
      Http()
        .newServerAt("0.0.0.0",
          Try(conf.getInt("address-service-port"))
            .toOption
            .getOrElse(80))
        .bind(service)
    }
}
