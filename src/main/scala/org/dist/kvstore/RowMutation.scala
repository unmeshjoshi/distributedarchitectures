package org.dist.kvstore

case class RowMutation(table:String, key:String, value:String)

case class RowMutationMessage(correlationId:Int, rowMutation:RowMutation)

case class QurorumResponse(messages:List[Message])

case class RowMutationResponse(correlationId:Int, key:String, success:Boolean)

case class QuorumResponse(values:List[RowMutationResponse])