package it.luca.aurora.configuration.metadata.extract

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Coordinates for extracting .avro data
 *
 * @param extractType type of data extraction
 * @param landingPath HDFS path where dataSource data are expected to land
 * @param fileNameRegex regex to be matched by files within landingPath
 */

case class AvroExtract(@JsonProperty(Extract.Type) override val extractType: String,
                       @JsonProperty(Extract.LandingPath) override val landingPath: String,
                       @JsonProperty(Extract.FileNameRegex) override val fileNameRegex: String)
  extends Extract(extractType, landingPath, fileNameRegex) {

  override protected def readDataFrame(sparkSession: SparkSession, path: String): DataFrame =
    sparkSession.read.format("avro").load(path)
}
