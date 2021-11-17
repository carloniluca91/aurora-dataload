package it.luca.aurora.app.job.extractor

import it.luca.aurora.configuration.metadata.extract.{AvroExtract, CsvExtract, Extract}
import it.luca.aurora.core.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DataExtractor[E <: Extract](protected val extract: E)
  extends Logging {

  /**
   * Read data at given HDFS path using a [[SparkSession]]
   * @param sparkSession application's sparkSession
   * @param path         HDFS path with data to be read
   * @return [[DataFrame]] read from given path
   */

  def extract(sparkSession: SparkSession, path: Path): DataFrame = {

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

object DataExtractor {

  def apply(extract: Extract): DataExtractor[_] = {

    extract match {
      case avro: AvroExtract => new AvroExtractor(avro)
      case csv: CsvExtract => new CsvExtractor(csv)
    }
  }
}
