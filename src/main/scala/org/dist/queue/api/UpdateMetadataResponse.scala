package org.dist.queue.api

case class UpdateMetadataResponse(val correlationId: Int,
                                  errorCode: Short = 0)