package org.dist.consensus.zab.api

object ClientRequestOrResponse {
  val SetDataKey: Short = 0
  val GetDataKey: Short = 1
}

case class SetDataRequest(path:String, data:String)
case class GetDataRequest(path:String)
case class SetDataResponse(path:String)
case class GetDataResponse(data:String)

case class ClientRequestOrResponse(val requestId: Short, val messageBodyJson: String, val correlationId: Int)
