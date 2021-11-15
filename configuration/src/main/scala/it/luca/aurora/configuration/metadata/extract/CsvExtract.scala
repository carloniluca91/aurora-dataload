package it.luca.aurora.configuration.metadata.extract

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Coordinates for extracting .csv data
 *
 * @param extractType type of data extraction
 * @param landingPath HDFS path where dataSource data are expected to land
 * @param fileNameRegex regex to be matched by files within landingPath
 * @param options Spark options for csv parsing
 */

case class CsvExtract(@JsonProperty(Extract.Type) override val extractType: String,
                      @JsonProperty(Extract.LandingPath) override val landingPath: String,
                      @JsonProperty(Extract.FileNameRegex) override val fileNameRegex: String,
                      options: Option[Map[String, String]])
  extends Extract(extractType, landingPath, fileNameRegex) {
}
