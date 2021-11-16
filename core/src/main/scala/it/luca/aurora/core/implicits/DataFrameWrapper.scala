package it.luca.aurora.core.implicits

import it.luca.aurora.core.Logging
import it.luca.aurora.core.implicits.DataFrameWrapper.{ApplicationId, ApplicationName, ApplicationStartDate, ApplicationStartTime, InsertDt, InsertTs}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DataFrameWrapper(private val dataFrame: DataFrame)
  extends Logging {

  /**
   * Add technical column related to the HDFS path of ingested file
   * @param columnName name for new column
   * @param filePath instance of [[Path]]
   * @return original dataFrame with column "input_file_path"
   */

  def withInputFilePathCol(columnName: String, filePath: Path): DataFrame =
    dataFrame.withColumn(columnName, lit(filePath.toString))

  /**
   * Repartition this [[DataFrame]] in order to produce output files not bigger than given file size
   * @param maxFileSizeInBytes maximum file size (in bytes)
   * @return repartitioned [[DataFrame]]
   */

  def withOptimizedRepartitioning(maxFileSizeInBytes: Int): DataFrame = {

    val persistedDataFrame: DataFrame = dataFrame.persist(StorageLevel.MEMORY_ONLY)
    val logicalPlan = persistedDataFrame.queryExecution.logical
    val dataFrameSizeInBytes: BigInt = persistedDataFrame.sparkSession
      .sessionState.executePlan(logicalPlan)
      .optimizedPlan.stats.sizeInBytes

    // Pick the maximum between 1 (allowed minimum) and the estimated number
    val numberOfPartitions: Int = math.max(math.ceil(dataFrameSizeInBytes.toDouble / maxFileSizeInBytes.toDouble).toInt, 1)
    log.info(s"Repartitioning given ${classOf[DataFrame].getSimpleName} into $numberOfPartitions partition(s)")
    persistedDataFrame.unpersist().coalesce(numberOfPartitions)
  }

  /**
   * Add some technical columns related to current Spark application to a [[DataFrame]]
   * @return original [[DataFrame]] plus some technical columns
   */

  def withTechnicalColumns(): DataFrame = {

    // Add technical columns
    val now = LocalDateTime.now()
    val sparkSession: SparkSession = dataFrame.sparkSession

    dataFrame
      .withColumn(InsertTs, lit(Timestamp.valueOf(now)))
      .withColumn(InsertDt, lit(now.format(DateTimeFormatter.ISO_LOCAL_DATE)))
      .withColumn(ApplicationId, lit(sparkSession.applicationId))
      .withColumn(ApplicationName, lit(sparkSession.appName))
      .withColumn(ApplicationStartTime, lit(sparkSession.startTimeAsTimestamp))
      .withColumn(ApplicationStartDate, lit(sparkSession.startTimeAsString("yyyy-MM-dd")))
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

object DataFrameWrapper {

  val InsertTs = "insert_ts"
  val InsertDt = "insert_dt"
  val ApplicationId = "application_id"
  val ApplicationName = "application_name"
  val ApplicationStartTime = "application_start_time"
  val ApplicationStartDate = "application_start_date"

  val TechnicalColumns: Seq[String] =
    InsertTs ::
      InsertDt ::
      ApplicationId ::
      ApplicationName ::
      ApplicationStartTime ::
      ApplicationStartDate :: Nil
}
