package lv.addresses.service

import java.io.File
import java.nio.file.{FileSystems, Path, Paths}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{HttpChallenge, `Tls-Session-Info`}
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Directive0, Directive1, Rejection, RejectionHandler, Route, ValidationRejection}
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.io.Source
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Try, Using}


private case object ReloadBlockedUsers

trait Authorization {

  private val conf = com.typesafe.config.ConfigFactory.load

  private val clientAuth = Try(conf.getBoolean("ssl.client-auth")).toOption.getOrElse(false)

  private val as = Boot.system

  protected val logger = Logger(LoggerFactory.getLogger("lv.addresses.service"))

  private val blockedUserActor: Option[ActorRef] =
    if (clientAuth) {
        if (conf.hasPath("ssl.blocked-users")) {
          val blockedUserFileName = conf.getString("ssl.blocked-users")
          if (new File(blockedUserFileName).exists())
            Some(as.actorOf(Props(classOf[AuthorizationActor], blockedUserFileName),
              "user-authorization"))
          else
            throw new Error(s"File for blocked users does not exist: $blockedUserFileName. " +
              s"Check ssl.blocked-users property value")
        } else {
          logger.warn("ssl.blocked-users property not specified, blocked users cannot be watched")
          None
        }
    } else None

  protected def checkUser(name: String) = blockedUserActor
    .map { _.ask(name)(1.second).mapTo[Boolean] }
    .getOrElse(Future.successful(true))

  protected def refreshBlockedUsers =
    blockedUserActor.map { ac =>
      ac ! ReloadBlockedUsers
      "Ok"
    }.getOrElse {
      s"Cannot refresh blocked users. Blocked user refresh process not activated. " +
        s"Check that property ssl.blocked-users points to existing file"
    }

  protected def authRejectionHandler = RejectionHandler.newBuilder().handle {
    case r: Rejection =>
      logger.warn(s"Unauthorized request. Details - $r")
      complete(StatusCodes.Unauthorized)
  }.result()

  protected def authenticateStrict: Directive1[String] =
    handleRejections(authRejectionHandler) &
      headerValueByType(`Tls-Session-Info`)
        .filter(_ => clientAuth, ValidationRejection("Unauthorized")) //must specify rejection otherwise passes auth rejection handler
        .flatMap { sessionInfo =>
          val session = sessionInfo.getSession()
          val principal = session.getPeerPrincipal
          val name = principal.getName
          onSuccess(checkUser(name))
            .flatMap { res =>
              if (res) provide(name)
              else reject(
                AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsRejected,
                  HttpChallenge("Any", "APP"))): Directive1[String]
            }
        }

  def authenticate: Directive0 =
    if (clientAuth) authenticateStrict.flatMap(_ => pass) else pass

  def reloadBlockedUsers: Route = (path("reload-blocked-users") & authenticateStrict) { admin =>
    Try(conf.getString("ssl.admin-name"))
      .collect { case u if u == admin => complete(refreshBlockedUsers) }
      .toOption
      .getOrElse(complete(StatusCodes.Unauthorized))
  }
}

class AuthorizationActor(blockedUserFileName: String) extends Actor {

  case class RefreshBlockedUsers(path: Path)

  val logger = Logger(LoggerFactory.getLogger("lv.addresses.service"))

  private val blockedUserFile = new File(blockedUserFileName)
  private val blockedUserFileDir: Path = Paths.get(blockedUserFile.getParent)

  private var users: Set[String] = _

  //launch watch service
  Future {
    if (blockedUserFileDir.toFile.isDirectory) {
      val watchService = FileSystems.getDefault().newWatchService()
      import java.nio.file.StandardWatchEventKinds._
      blockedUserFileDir.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE)
      var poll = true
      while (poll) {
        val key = watchService.take()
        key.pollEvents().asScala foreach { ev =>
          self ! RefreshBlockedUsers(ev.context().asInstanceOf[Path])
        }
        poll = key.reset()
      }
      logger.error(s"Blocked users service watcher terminated, " +
        s"server will have to be restarted if new blocked user is added or " +
        s"reload-blocked-users service invoked manually.")
    } else {
      logger.error(s"Directory $blockedUserFileDir does not exist. " +
        s"Cannot watch for blocked users file $blockedUserFileName changes.")
    }
  }(context.dispatcher)

  protected def refreshUsers() = {
    if (blockedUserFile.exists()) {
      Using(Source.fromFile(blockedUserFile, "UTF-8")) { _
        .getLines()
        .foldLeft(Set[String]()) { (res, usr) => res + usr }
      }.get
    } else {
      logger.error(s"Blocked user file $blockedUserFile does not exist.")
      Set[String]()
    }
  }

  override def preStart(): Unit = {
    logger.info(s"Blocked user monitoring module started")
    users = Try(refreshUsers()).recover {
      case e: Exception =>
        logger.error("Unable to load blocked users", e)
        Set[String]()
    }.get
  }

  override def receive: Receive = {
    def refresh = {
      logger.info(s"Updating blocked users")
      users = refreshUsers()
    }
    {
      case ReloadBlockedUsers => refresh
      case RefreshBlockedUsers(path) => if (path.toFile.getName == blockedUserFile.getName) refresh
      case user: String => sender() ! (!users(user)) //user is valid if it is not present in blocked user list
    }
  }

  override def postStop(): Unit = {
    logger.info(s"Authorization module terminated")
  }
}
