package it.luca.aurora.core.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import it.luca.aurora.core.logging.Logging

import java.io.{File, IOException}

object ObjectDeserializer
  extends Logging {

  object DataFormat extends Enumeration {

    type DataFormat = Value
    val Json, Yaml = Value
  }

  protected final val jsonMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  protected final val yamlMapper = new YAMLMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  /**
   * Converts a file to instance of type [[T]]
   * @param file input file
   * @param tClass class of deserialized object
   * @tparam T type of deserialized object
   * @throws java.io.IOException if deserialization fails
   * @return instance of type [[T]]
   */

  @throws[IOException]
  def deserializeFile[T](file: File, tClass: Class[T]): T = {

    val (fileName, className) = (file.getName, tClass.getSimpleName)
    val mapper = if (fileName.toLowerCase.endsWith(".json")) jsonMapper else yamlMapper
    log.info("Deserializing file {} as an instance of {}", fileName, className)
    val instance = mapper.readValue(file, tClass)
    log.info("Successfully deserialized file {} as an instance of {}", fileName, className)
    instance
  }

  /**
   * Converts a string to instance of type [[T]]
   * @param string input string
   * @param tClass class of deserialized object
   * @param dataFormat a value of [[DataFormat]]
   * @tparam T type of deserialized object
   * @throws java.io.IOException if deserialization fails
   * @return instance of type [[T]]
   */

  @throws[IOException]
  def deserializeString[T](string: String, tClass: Class[T], dataFormat: DataFormat.Value): T = {

    val dataFormatName = dataFormat.toString
    val className = tClass.getSimpleName
    val mapper = dataFormat match {
      case DataFormat.Json => jsonMapper
      case DataFormat.Yaml => yamlMapper
    }

    log.info("Deserializing given {} string as an instance of {}", dataFormatName, className)
    val instance = mapper.readValue(string, tClass)
    log.info("Successfully deserialized given {} string as an instance of {}", dataFormatName, className)
    instance
  }

}
