package it.luca.aurora.app.job.extractor

import it.luca.aurora.configuration.metadata.extract.AvroExtract
import org.apache.spark.sql.{DataFrame, SparkSession}

class AvroExtractor(override protected val extract: AvroExtract)
  extends DataExtractor[AvroExtract](extract) {

  override protected def readDataFrame(sparkSession: SparkSession, path: String): DataFrame =
    sparkSession.read.format("avro").load(path)
}
