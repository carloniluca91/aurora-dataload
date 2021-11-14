package it.luca.aurore.configuration

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import it.luca.aurora.core.Logging

import java.io.IOException
import scala.io.{BufferedSource, Source}

object JsonDeserializer
  extends Logging {

  protected final val mapper: ObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)

  /**
   * Deserialize given file as as instance of type T
   * @param fileName file name
   * @param tClass class of deserialized instance
   * @tparam T type of deserialized instance
   * @throws IOException if deserialization fails
   * @return instance of T
   */

  @throws[IOException]
  def deserializeFileAs[T](fileName: String, tClass: Class[T]): T =
    deserializeStreamAs(Source.fromFile(fileName), s"input file $fileName", tClass)

  /**
   * Deserialize given resource as as instance of type T
   * @param fileName resource name
   * @param tClass class of deserialized instance
   * @tparam T type of deserialized instance
   * @throws IOException if deserialization fails
   * @return instance of T
   */

  @throws[IOException]
  def deserializeResourceAs[T](fileName: String, tClass: Class[T]): T = {
    val source = Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream(fileName))
    deserializeStreamAs(source, s"input resource $fileName", tClass)
  }

  /**
   * Deserialize given source as as instance of type T
   * @param source input [[Source]]
   * @param tClass class of deserialized instance
   * @tparam T type of deserialized instance
   * @throws IOException if deserialization fails
   * @return instance of T
   */

  @throws[IOException]
  protected final def deserializeStreamAs[T](source: BufferedSource, inputDescription: String, tClass: Class[T]): T = {

    val description = s"$inputDescription as an instance of ${tClass.getSimpleName}"
    log.info(s"Deserializing $description")
    val instance: T = mapper.readValue(source.reader(), tClass)
    log.info(s"Successfully deserialized $description")
    instance
  }
}
