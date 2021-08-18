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
    log.info(s"Deserializing file $fileName as an instance of $className")
    val deserializingFunction: File => T =
      if (fileName.toLowerCase.endsWith(".yaml")) yamlMapper.readValue(_, c)
      else jsonMapper.readValue(_, c)

    val instance: T = deserializingFunction(file)
    log.info(s"Successfully deserialized file $fileName as an instance of $className")
    instance
  }
}