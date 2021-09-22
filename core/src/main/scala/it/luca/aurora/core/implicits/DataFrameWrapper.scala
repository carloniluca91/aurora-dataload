package it.luca.aurora.core.implicits

import it.luca.aurora.core.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DataFrameWrapper(private val dataFrame: DataFrame)
  extends Logging {

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
    persistedDataFrame.coalesce(numberOfPartitions)
  }

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

    dataFrame
      .withColumn("insert_ts", lit(nowAsTimestamp))
      .withColumn("insert_dt", lit(now.format(DateTimeFormatter.ISO_LOCAL_DATE)))
      .withColumn("application_id", lit(sparkContext.applicationId))
      .withColumn("application_name", lit(sparkContext.appName))
      .withColumn("application_start_time", lit(sparkContext.startTimeAsTimestamp))
      .withColumn("application_start_date", lit(sparkContext.startTimeAsString("yyyy-MM-dd")))
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
