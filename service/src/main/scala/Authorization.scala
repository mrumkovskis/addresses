package lv.addresses.service

import java.io.File
import java.nio.file.{FileSystems, Path, Paths, StandardWatchEventKinds, WatchEvent}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.headers.{HttpChallenge, `Tls-Session-Info`}
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Directive0}
import akka.http.scaladsl.server.Directives.{headerValueByType, pass, reject}
import akka.pattern.ask
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import akka.http.scaladsl.server.directives.FutureDirectives._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.io.Source
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try


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

  def checkUser(name: String) = userActor
    .map { _.ask(name)(1.second).mapTo[Boolean] }
    .getOrElse(Future.successful(false))

  def authenticate =
    if (clientAuth) {
      headerValueByType(`Tls-Session-Info`).flatMap { sessionInfo =>
        val session = sessionInfo.getSession()
        val principal = session.getPeerPrincipal
        val name = principal.getName
        onSuccess(checkUser(name))
          .flatMap { res =>
            if (res) pass
            else reject(
              AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsRejected,
                HttpChallenge("Any", "APP"))): Directive0
          }
      }
    } else pass
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
    case RefreshUsers(path) =>
      if (path.toFile.getName == userFile.getName) {
        logger.info(s"Updating users")
        refreshUsers()
      }

    case user: String =>
      sender() ! users(user)
  }
}
