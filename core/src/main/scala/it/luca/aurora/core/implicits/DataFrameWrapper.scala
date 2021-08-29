package it.luca.aurora.core.implicits

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}

class DataFrameWrapper(private val dataFrame: DataFrame) {

  /**
   * Add technical column related to the HDFS path of ingested file
   * @param filePath instance of [[Path]]
   * @return original dataFrame with column "input_file_path"
   */

  def withInputFilePathCol(filePath: Path): DataFrame =
    dataFrame.withColumn("input_file_path", lit(filePath.toString))

  /**
   * Add some technical columns related to current Spark application to a [[DataFrame]]
   * @return original [[DataFrame]] plus some technical columns
   */

  def withTechnicalColumns(): DataFrame = {

    // Add technical columns
    val now = LocalDateTime.now()
    val nowAsTimestamp: Timestamp = Timestamp.valueOf(now)
    val sparkContext: SparkContext = dataFrame.sparkSession.sparkContext
    val applicationStartTime = Timestamp.from(Instant.ofEpochMilli(sparkContext.startTime))

    dataFrame
      .withColumn("insert_ts", lit(nowAsTimestamp))
      .withColumn("insert_dt", lit(now.format(DateTimeFormatter.ISO_LOCAL_DATE)))
      .withColumn("application_id", lit(sparkContext.applicationId))
      .withColumn("application_name", lit(sparkContext.appName))
      .withColumn("application_start_time", lit(applicationStartTime))
      .withColumn("application_start_date", lit(applicationStartTime
        .toLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE)))
  }

  /**
   * Rename all dataFrame [[DataFrame]] columns according to SQL naming convention
   * @return original [[DataFrame]] with columns renamed according to SQL naming convention
   *         (e.g, column 'applicationStartTime' becomes 'application_start_time)
   */

  def withSqlNamingConvention(): DataFrame = {

    dataFrame.columns.foldLeft(dataFrame) {
      case (df, columnName) =>
        df.withColumnRenamed(columnName, columnName.replaceAll("[A-Z]", "_$0").toLowerCase)}
  }
}
