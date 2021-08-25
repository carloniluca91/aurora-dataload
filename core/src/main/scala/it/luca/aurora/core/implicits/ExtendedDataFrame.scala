package it.luca.aurora.core.implicits

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

class ExtendedDataFrame(dataFrame: DataFrame) {

  /**
   * Add technical columns to a [[org.apache.spark.sql.DataFrame]]
   * @return original [[org.apache.spark.sql.DataFrame]] plus some technical columns
   */

  def withTechnicalColumns(): DataFrame = {

    // Add technical columns
    val now = LocalDateTime.now()
    val nowAsTimestamp: Timestamp = Timestamp.valueOf(now)
    val sparkContext: SparkContext = dataFrame.sparkSession.sparkContext
    val applicationStartTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(sparkContext.startTime), ZoneId.systemDefault())

    dataFrame
      .withColumn("insert_ts", lit(nowAsTimestamp))
      .withColumn("insert_dt", lit(now.format(DateTimeFormatter.ISO_LOCAL_DATE)))
      .withColumn("application_id", lit(sparkContext.applicationId))
      .withColumn("application_name", lit(sparkContext.appName))
      .withColumn("application_start_time", lit(Timestamp.valueOf(applicationStartTime)))
      .withColumn("application_start_date", lit(applicationStartTime.format(DateTimeFormatter.ISO_LOCAL_DATE)))
  }

  /**
   * Rename all dataFrame [[org.apache.spark.sql.DataFrame]] columns according to SQL naming convention
   * @return original [[org.apache.spark.sql.DataFrame]] with columns renamed according to SQL naming convention
   *         (e.g, column 'applicationStartTime' becomes 'application_start_time)
   */

  def withSqlNamingConvention(): DataFrame = {

    dataFrame.columns.foldLeft(dataFrame) {
      case (df, columnName) =>
        df.withColumnRenamed(columnName, columnName.replaceAll("[A-Z]", "_$0").toLowerCase)}
  }
}
