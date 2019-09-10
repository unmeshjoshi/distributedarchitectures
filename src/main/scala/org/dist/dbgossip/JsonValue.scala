package org.dist.dbgossip

import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

/**
 * A simple wrapper over Jackson's JsonNode that enables type safe parsing via the `DecodeJson` type
 * class.
 *
 * Typical usage would be something like:
 *
 * {{{
 * val jsonNode: JsonNode = ???
 * val jsonObject = JsonValue(jsonNode).asJsonObject
 * val intValue = jsonObject("int_field").to[Int]
 * val optionLongValue = jsonObject("option_long_field").to[Option[Long]]
 * val mapStringIntField = jsonObject("map_string_int_field").to[Map[String, Int]]
 * val seqStringField = jsonObject("seq_string_field").to[Seq[String]
 * }}}
 *
 * The `to` method throws an exception if the value cannot be converted to the requested type. An alternative is the
 * `toEither` method that returns an `Either` instead.
 */
trait JsonValue {

  protected def node: JsonNode

  /**
   * Decode this JSON value into an instance of `T`.
   *
   * @throws JsonMappingException if this value cannot be decoded into `T`.
   */
  def to[T](implicit decodeJson: DecodeJson[T]): T = decodeJson.decode(node)

  /**
   * Decode this JSON value into an instance of `Right[T]`, if possible. Otherwise, return an error message
   * wrapped by an instance of `Left`.
   */
  def toEither[T](implicit decodeJson: DecodeJson[T]): Either[String, T] = decodeJson.decodeEither(node)

  /**
   * If this is a JSON object, return an instance of JsonObject. Otherwise, throw a JsonMappingException.
   */
  def asJsonObject: JsonObject =
    asJsonObjectOption.getOrElse(throw new JsonMappingException(null, s"Expected JSON object, received $node"))

  /**
   * If this is a JSON object, return a JsonObject wrapped by a `Some`. Otherwise, return None.
   */
  def asJsonObjectOption: Option[JsonObject] = this match {
    case j: JsonObject => Some(j)
    case _ => node match {
      case n: ObjectNode => Some(new JsonObject(n))
      case _ => None
    }
  }

  /**
   * If this is a JSON array, return an instance of JsonArray. Otherwise, throw a JsonMappingException.
   */
  def asJsonArray: JsonArray =
    asJsonArrayOption.getOrElse(throw new JsonMappingException(null, s"Expected JSON array, received $node"))

  /**
   * If this is a JSON array, return a JsonArray wrapped by a `Some`. Otherwise, return None.
   */
  def asJsonArrayOption: Option[JsonArray] = this match {
    case j: JsonArray => Some(j)
    case _ => node match {
      case n: ArrayNode => Some(new JsonArray(n))
      case _ => None
    }
  }

  override def hashCode: Int = node.hashCode

  override def equals(a: Any): Boolean = a match {
    case a: JsonValue => node == a.node
    case _ => false
  }

  override def toString: String = node.toString

}

object JsonValue {

  /**
   * Create an instance of `JsonValue` from Jackson's `JsonNode`.
   */
  def apply(node: JsonNode): JsonValue = node match {
    case n: ObjectNode => new JsonObject(n)
    case n: ArrayNode => new JsonArray(n)
    case _ => new BasicJsonValue(node)
  }

  private class BasicJsonValue (protected val node: JsonNode) extends JsonValue

}

