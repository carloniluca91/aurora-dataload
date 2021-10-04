package it.luca.aurora.core.implicits

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}

import java.sql.Timestamp
import java.time.Instant
import java.time.format.DateTimeFormatter

class SparkSessionWrapper(protected val sparkSession: SparkSession) {
  
  /**
   * Get application id
   * @return
   */

  def applicationId: String = sparkSession.sparkContext.applicationId

  /**
   * Get application name
   * @return
   */

  def appName: String = sparkSession.sparkContext.appName
  
  /**
   * Get underlying instance of [[FileSystem]]
   * @return instance of [[FileSystem]]
   */
    
  def getFileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  /**
   * Get the location of a table
   * @param fqTableName fully qualified (i.e. db.table) table name
   * @return HDFS location of given table
   */

  def getTableLocation(fqTableName: String): String = {

    sparkSession.sql(s"DESCRIBE FORMATTED $fqTableName")
      .filter(lower(col("col_name")) === "location")
      .select(col("data_type"))
      .collect.head
      .getAs[String](0)
  }


  /**
   * Get start time of current Spark application as [[Timestamp]]
   * @return [[Timestamp]] representing start time of current Spark application
   */

  def startTimeAsTimestamp : Timestamp = Timestamp.from(
    Instant.ofEpochMilli(sparkSession.sparkContext.startTime))

  /**
   * Get start time of current Spark application as a string with given pattern
   * @param pattern pattern for output string
   * @return string representing start time of current Spark application
   */

  def startTimeAsString(pattern: String): String = startTimeAsTimestamp
    .toLocalDateTime.format(DateTimeFormatter.ofPattern(pattern))
}
