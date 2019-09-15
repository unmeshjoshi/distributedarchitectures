package org.dist.kvstore

import com.fasterxml.jackson.annotation.{JsonAutoDetect, PropertyAccessor}
import com.fasterxml.jackson.databind.{DeserializationFeature, JavaType, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonSerDes {
  val objectMapper = new ObjectMapper()
  objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  objectMapper.registerModule(DefaultScalaModule)

  def serialize(obj:Any):String = {
    objectMapper.writeValueAsString(obj)
  }

  def deserialize[T](json:Array[Byte], clazz:Class[T]):T = {
    objectMapper.readValue(json, clazz)
  }
}
