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
import scala.util.Try


private case object ReloadUsers

trait Authorization {

  private val conf = com.typesafe.config.ConfigFactory.load

  private val clientAuth = Try(conf.getBoolean("ssl.client-auth")).toOption.getOrElse(false)

  private val as = ActorSystem("user-authorization")

  private val userActor: Option[ActorRef] =
    if (clientAuth) {
      val userFileName =
        Try(conf.getString("ssl.authorized-users")).toOption.getOrElse {
          throw new Error(s"User file not specified in configuration property ssl.authorized-users")
        }
      Some(as.actorOf(Props(classOf[AuthorizationActor], userFileName), "user-authorization"))
    } else None

  protected def checkUser(name: String) = userActor
    .map { _.ask(name)(1.second).mapTo[Boolean] }
    .getOrElse(Future.successful(false))

  protected def refreshUsers =
    userActor.map { ac =>
      ac ! ReloadUsers
      "Ok"
    }.getOrElse {
      if (clientAuth) {
        s"User file not specified in configuration property ssl.authorized-users"
      } else {
        s"User file not specified in configuration property ssl.authorized-users"    }
    }

  protected def authRejectionHandler = RejectionHandler.newBuilder().handle {
    case r: Rejection =>
      as.log.warning(s"Unauthorized request. Details - $r")
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
    handleRejections(authRejectionHandler) &
      ((if (clientAuth) authenticateStrict.flatMap(_ => pass) else pass): Directive0) // somehow cast is needed?

  def reloadUsers: Route = (path("reload-users") & authenticateStrict) { admin =>
    Try(conf.getString("ssl.admin-name"))
      .collect { case u if u == admin => complete(refreshUsers) }
      .toOption
      .getOrElse(complete(StatusCodes.Unauthorized))
  }
}

class AuthorizationActor(userFileName: String) extends Actor {

  case class RefreshUsers(path: Path)

  val logger = Logger(LoggerFactory.getLogger("lv.addresses.service"))

  private val userFile = new File(userFileName)
  private val userFileDir: Path = Paths.get(userFile.getParent)

  if (!userFileDir.toFile.isDirectory) {
    throw new Error(s"Directory $userFileDir does not exist. " +
      s"Cannot watch for authorization file $userFileName changes.")
  }

  private var users: Set[String] = _

  //launch watch service
  Future {
    val watchService = FileSystems.getDefault().newWatchService()
    import java.nio.file.StandardWatchEventKinds._
    userFileDir.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE)
    var poll = true
    while (poll) {
      val key = watchService.take()
      key.pollEvents().asScala foreach { ev =>
        self ! RefreshUsers(ev.context().asInstanceOf[Path])
      }
      poll = key.reset()
    }
    logger.error(s"user service watcher terminated, server will have to be restarted if new user is added")
  }(context.dispatcher)

  protected def refreshUsers() = {
    if (userFile.exists()) {
      Source.fromFile(userFile, "UTF-8")
        .getLines()
        .foldLeft(Set[String]()) { (res, usr) => res + usr }
    } else {
      logger.error(s"User authorization file $userFile does not exist. Authorization not possible.")
      Set[String]()
    }
  }

  override def preStart(): Unit = {
    logger.info(s"Authorization module started")
    users = refreshUsers()
  }

  override def receive: Receive = {
    def refresh = {
      logger.info(s"Updating users")
      users = refreshUsers()
    }
    {
      case ReloadUsers => refresh
      case RefreshUsers(path) => if (path.toFile.getName == userFile.getName) refresh
      case user: String => sender() ! users(user)
    }
  }

  override def postStop(): Unit = {
    logger.info(s"Authorization module terminated")
  }
}
