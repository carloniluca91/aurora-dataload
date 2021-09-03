package it.luca.aurora.core.implicits

import org.apache.spark.SparkContext

import java.sql.Timestamp
import java.time.Instant
import java.time.format.DateTimeFormatter

class SparkContextWrapper(private val sc: SparkContext) {

  def applicationId: String = sc.applicationId

  def appName: String = sc.appName

  /**
   * Get start time of current Spark application as [[Timestamp]]
   * @return [[Timestamp]] representing start time of current Spark application
   */

  def startTimeAsTimestamp : Timestamp = Timestamp.from(Instant.ofEpochMilli(sc.startTime))

  /**
   * Get start time of current Spark application as a string with given pattern
   * @param pattern pattern for output string
   * @return string representing start time of current Spark application
   */

  def startTimeAsString(pattern: String): String = startTimeAsTimestamp
    .toLocalDateTime.format(DateTimeFormatter.ofPattern(pattern))
}
