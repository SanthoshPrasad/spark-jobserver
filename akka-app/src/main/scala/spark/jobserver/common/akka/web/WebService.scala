package spark.jobserver.common.akka.web

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import javax.net.ssl.SSLContext
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Contains methods for starting an embedded Spray web server.
 */
object WebService{
  implicit val system: ActorSystem = ActorSystem(UUID.randomUUID().toString)
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
  implicit val dispatcher = system.dispatcher
  val sslConfig = AkkaSSLConfig()
  /**
   * Starts a web server given a Route.  Note that this call is meant to be made from an App or other top
   * level scope, and not within an actor, as system.actorOf may block.
   *
   * @param route The spray Route for the service.  Multiple routes can be combined like (route1 ~ route2).
   * @param system the ActorSystem to use
   * @param host The host string to bind to, defaults to "0.0.0.0"
   * @param port The port number to bind to
   */
  def start(routes: Route, system: ActorSystem,
            host: String = "0.0.0.0", port: Int = 8080)(implicit sslContext: SSLContext) {

    implicit val actorSystem: ActorSystem = system
    val logger = LoggerFactory.getLogger(getClass)

   /* commented as jobserver not working with this
      val https: HttpsConnectionContext = ConnectionContext.https(sslContext)
      Http().setDefaultServerHttpContext(https)
      Http().bindAndHandle(routes,
      interface = host,
      port = port,
      connectionContext = https)*/

    val webServiceFuture = Http().bindAndHandle(routes,
      interface = host,
      port = port)

    webServiceFuture onComplete{
      case Success(value) => logger.info("job server  start up succeess : {}", value.toString)
      case Failure(exception) => logger.error("job server  start up failed ", exception)
    }
  }




}
