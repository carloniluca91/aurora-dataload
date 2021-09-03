package it.luca.aurora.core.implicits

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

class SparkSessionWrapper(protected val sparkSession: SparkSession) {

  /**
   * Get underlying instance of [[FileSystem]]
   * @return instance of [[FileSystem]]
   */

  def getFileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

}
