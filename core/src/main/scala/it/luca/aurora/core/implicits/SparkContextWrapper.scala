package it.luca.aurora.core.implicits

import org.apache.spark.SparkContext

import java.sql.Timestamp
import java.time.Instant
import java.time.format.DateTimeFormatter

class SparkContextWrapper(private val sc: SparkContext) {

  def applicationId: String = sc.applicationId

  def appName: String = sc.appName

  def startTimeAsTimestamp : Timestamp = Timestamp.from(Instant.ofEpochMilli(sc.startTime))

  def startTimeAsString(pattern: String): String = startTimeAsTimestamp
    .toLocalDateTime.format(DateTimeFormatter.ofPattern(pattern))
}
