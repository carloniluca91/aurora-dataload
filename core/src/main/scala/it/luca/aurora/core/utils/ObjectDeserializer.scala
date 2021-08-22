package it.luca.aurora.core.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import it.luca.aurora.core.logging.Logging

import java.io.File

object ObjectDeserializer
  extends Logging {

  private final val jsonMapper: ObjectMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private final val yamlMapper: ObjectMapper = new YAMLMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def deserialize[T](file: File, c: Class[T]): T = {

    val (fileName, className): (String, String) = (file.getName, c.getSimpleName)
    val mapper: ObjectMapper = if (fileName.toLowerCase.endsWith(".json")) jsonMapper else yamlMapper
    log.info(s"Deserializing file $fileName as an instance of $className")
    val instance: T = mapper.readValue(file, c)
    log.info(s"Successfully deserialized file $fileName as an instance of $className")
    instance
  }
}