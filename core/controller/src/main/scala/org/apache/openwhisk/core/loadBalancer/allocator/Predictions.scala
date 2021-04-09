package org.apache.openwhisk.core.loadBalancer.allocator

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.{DeadLetterSuppression, NoSerializationVerificationNeeded}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity.Strict
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

final class Prediction(val current: immutable.Seq[TenantProportion], val future: immutable.Seq[TenantProportion])
    extends DeadLetterSuppression
    with NoSerializationVerificationNeeded {}

final class TenantProportion(tenantName: String, proportion: Double) extends NoSerializationVerificationNeeded

/**
{
    "current" => ["tenant1": 0.1}, {"tenant2": 0.9}]
    "future" => [{"tenant1": 0.2}, {"tenant2": 0.8}]
 }
 */
object Prediction {
  final case class Prediction(current: immutable.Map[String, Double], future: immutable.Map[String, Double])
}

class Predictions(system: ActorSystem[Nothing]) {

  implicit val actorSystem = system
  private val http = Http()(system)
  private val log = system.log

  private def predictionsRequest =
    for {
      host <- Option(system.settings.config.getString("akka.allocator.predictions-host"))
      port <- Option(system.settings.config.getInt("akka.allocator.predictions-port"))
    } yield {
      val path = Uri.Path.Empty / "api" / "v2" / "predictions"
      val uri = Uri
        .from("http", "", host, port)
        .withPath(path)

      HttpRequest(uri = uri)
    }

  private def optionToFuture[T](option: Option[T], failMsg: String): Future[T] =
    option.fold(Future.failed[T](new NoSuchElementException(failMsg)))(Future.successful)

  def get(resolveTimeout: FiniteDuration): Future[Prediction.Prediction] = {
    import JsonFormat._

    implicit val executionContext: ExecutionContext = system.executionContext

    for {
      request <- optionToFuture(predictionsRequest, s"Unable to form predictions request")

      response: HttpResponse <- http.singleRequest(request)

      entity: Strict <- response.entity.toStrict(resolveTimeout)

      predictions <- {

        response.status match {
          case StatusCodes.OK =>
            val unmarshalled = Unmarshal(entity).to[Prediction.Prediction]
            unmarshalled.failed.foreach { t =>
              log.warn(
                "Failed to unmarshal Predictions API response.  Status code: [{}]; Response body: [{}]. Ex: [{}]",
                response.status.value,
                entity,
                t.getMessage)
            }
            unmarshalled
          case StatusCodes.Forbidden =>
            Unmarshal(entity).to[String].foreach { body =>
              log
                .warn("Forbidden to communicate with Predictions API server; check RBAC settings. Response: [{}]", body)
            }
            Future.failed(new Exception("Forbidden when communicating with the Kubernetes API."))
          case other =>
            Unmarshal(entity).to[String].foreach { body =>
              log.warn2(
                "Non-200 when communicating with Predictions API server. Status code: [{}]. Response body: [{}]",
                other,
                body)
            }

            Future.failed(new Exception(s"Non-200 from Predictions API server: $other"))
        }

      }
    } yield {
      predictions
    }
  }
}
