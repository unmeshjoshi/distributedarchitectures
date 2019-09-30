package org.dist.kvstore

case class RowMutation(table:String, key:String, value:String)

case class RowMutationMessage(correlationId:Int, rowMutation:RowMutation)
