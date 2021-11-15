package it.luca.aurora.configuration.metadata.extract

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import it.luca.aurora.configuration.Dto

/**
 * Base class representing coordinates for data extraction
 * @param extractType type of data extraction
 * @param landingPath HDFS path where dataSource data are expected to land
 * @param fileNameRegex regex to be matched by files within landingPath
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
  property = Extract.Type,
  visible = true)
@JsonSubTypes(Array(new JsonSubTypes.Type(value = classOf[AvroExtract], name = Extract.Avro),
  new JsonSubTypes.Type(value = classOf[CsvExtract], name = Extract.Csv)))
abstract class Extract(val extractType: String,
                       val landingPath: String,
                       val fileNameRegex: String)
  extends Dto {

  required(extractType, Extract.Type)
  required(landingPath, Extract.LandingPath)
  required(fileNameRegex, Extract.FileNameRegex)
}

object Extract {

  final val Avro = "avro"
  final val Csv = "csv"

  final val FileNameRegex = "fileNameRegex"
  final val LandingPath = "landingPath"
  final val Type = "type"
}