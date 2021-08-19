package it.luca.aurora.core.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import it.luca.aurora.core.logging.Logging

import java.io.File

object ObjectDeserializer
  extends Logging {

  private final val mapper: ObjectMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def deserialize[T](file: File, c: Class[T]): T = {

    val (fileName, className): (String, String) = (file.getName, c.getSimpleName)
    log.info(s"Deserializing file $fileName as an instance of $className")
    val instance: T = mapper.readValue(file, c)
    log.info(s"Successfully deserialized file $fileName as an instance of $className")
    instance
  }
}