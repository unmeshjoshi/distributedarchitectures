package org.dist.queue.message

import java.io.{InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

object CompressionFactory {

  def apply(compressionCodec: CompressionCodec, stream: OutputStream): OutputStream = {
    compressionCodec match {
      case DefaultCompressionCodec => new GZIPOutputStream(stream)
      case GZIPCompressionCodec => new GZIPOutputStream(stream)
//      case SnappyCompressionCodec =>
//
//        new SnappyOutputStream(stream)
//      case _ =>
        throw new RuntimeException("Unknown Codec: " + compressionCodec)
    }
  }

  def apply(compressionCodec: CompressionCodec, stream: InputStream): InputStream = {
    compressionCodec match {
      case DefaultCompressionCodec => new GZIPInputStream(stream)
      case GZIPCompressionCodec => new GZIPInputStream(stream)
//      case SnappyCompressionCodec =>
//
//        new SnappyInputStream(stream)
      case _ =>
        throw new RuntimeException("Unknown Codec: " + compressionCodec)
    }
  }
}
