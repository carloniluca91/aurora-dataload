package it.luca.aurora.core.implicits

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

class SparkSessionWrapper(protected val sparkSession: SparkSession) {

  def getFileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

}
