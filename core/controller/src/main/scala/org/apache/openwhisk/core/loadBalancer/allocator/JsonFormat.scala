package org.apache.openwhisk.core.loadBalancer.allocator

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.openwhisk.core.loadBalancer.allocator.Allocation.ReplyError
import org.apache.openwhisk.core.loadBalancer.allocator.PodList._
import spray.json._

object JsonFormat extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val containerPortFormat: JsonFormat[ContainerPort] = jsonFormat2(ContainerPort)
  implicit val containerFormat: JsonFormat[Container] = jsonFormat2(Container)
  implicit val podSpecFormat: JsonFormat[PodSpec] = jsonFormat2(PodSpec)
  implicit val podStatusFormat: JsonFormat[PodStatus] = jsonFormat2(PodStatus)
  implicit val metadataFormat: JsonFormat[Metadata] = jsonFormat1(Metadata)
  implicit val podFormat: JsonFormat[Pod] = jsonFormat3(Pod)
  implicit val podListFormat: RootJsonFormat[PodList] = jsonFormat1(PodList.apply)
  implicit val replyErrorFormat: JsonFormat[ReplyError] = jsonFormat1(ReplyError)
  implicit val prediction: RootJsonFormat[Prediction.Prediction] = jsonFormat2(Prediction.Prediction)
}
