package it.luca.aurora.app.job.extractor

import it.luca.aurora.configuration.metadata.extract.CsvExtract
import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvExtractor(override protected val extract: CsvExtract)
  extends DataExtractor[CsvExtract](extract) {

  override protected def readDataFrame(sparkSession: SparkSession, path: String): DataFrame = {

    (extract.options match {
      case Some(value) => sparkSession.read.options(value)
      case None => sparkSession.read
    }).csv(path)
  }
}
