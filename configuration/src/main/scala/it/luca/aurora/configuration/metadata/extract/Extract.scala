package it.luca.aurora.configuration.metadata.extract

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import it.luca.aurora.configuration.Dto
import it.luca.aurora.core.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  extends Dto
    with Logging {

  required(extractType, Extract.Type)
  required(fileNameRegex, Extract.FileNameRegex)

  /**
   * Read data at given HDFS path using a [[SparkSession]]
   * @param sparkSession application's sparkSession
   * @param path         HDFS path with data to be read
   * @return [[DataFrame]] read from given path
   */

  def read(sparkSession: SparkSession, path: Path): DataFrame = {

    val fileName: String = path.getName
    log.info(s"Starting to read input file $fileName")
    val dataFrame: DataFrame = readDataFrame(sparkSession, path.toString)
    log.info(s"Successfully read input file $fileName. Schema:\n\n${dataFrame.schema.treeString}")
    dataFrame
  }

  /**
   * Return a [[DataFrame]] from a string representing an HDFS path using a [[SparkSession]]
   * @param sparkSession application's SparkSession
   * @param path HDFS path from which data will be read
   * @return [[DataFrame]] containing data from given HDFS path
   */

  protected def readDataFrame(sparkSession: SparkSession, path: String): DataFrame

}

object Extract {

  final val Avro = "avro"
  final val Csv = "csv"

  final val FileNameRegex = "fileNameRegex"
  final val LandingPath = "landingPath"
  final val Type = "type"
}