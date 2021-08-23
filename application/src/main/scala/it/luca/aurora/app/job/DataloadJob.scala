package it.luca.aurora.app.job

import it.luca.aurora.app.utils.Utils.timestampToString

import java.sql.{Connection, SQLException, Timestamp}
import it.luca.aurora.core.configuration.metadata.DataSourceMetadata
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DataloadJob(protected val sparkSession: SparkSession,
                  protected val dataSourceMetadata: DataSourceMetadata,
                  protected val impalaJDBCConnection: Connection)
  extends Logging {

  def run(inputFile: FileStatus): Unit = {

    Try {

      val inputFilePath: String = inputFile.getPath.toString
      val inputDataFrame: DataFrame = sparkSession.read
        .options(Map("header" -> "true"))
        .csv(inputFilePath)

      log.info(s"Successfully read input file $inputFilePath")
      val filterCols: Seq[(String, Column)] = dataSourceMetadata.getFilters
        .toSeq.map { x => (x, SqlExpressionParser.parse(x)) }

      val overAllFilterCol: Column = filterCols.map{ _._2 }.reduce(_ && _)
      val dataSourceId: String = dataSourceMetadata.getId
      log.info(s"Successfully parsed all of ${dataSourceMetadata.getFilters.size()} filter(s) for dataSource $dataSourceId")

      // Invalid records (i.e. that do not satisfy all of dataSource filters)
      val filterFailureCols: Seq[Column] = filterCols.map { x => when(x._2, x._1) }
      val invalidRecordsDataFrame: DataFrame = inputDataFrame.filter(!overAllFilterCol)
        .withColumn("failed_checks", concat_ws(", ", filterFailureCols: _*))

      // Valid records
      val columnsToAdd: Seq[(String, Column)] = dataSourceMetadata.getTrasformations
        .toSeq.map { x => (x.getAlias, SqlExpressionParser.parse(x.getExpression)) }
      val numberOfTransformations: Int = dataSourceMetadata.getTrasformations.size()
      log.info(s"Successfully converted all of $numberOfTransformations trasformation(s) for dataSource $dataSourceId")
      val validRecordsDataFrame: DataFrame = columnsToAdd
        .foldLeft(inputDataFrame.filter(overAllFilterCol)) {
          (df, tuple) => df.withColumn(tuple._1, tuple._2)
      }

      log.info(s"Successfully added all of $numberOfTransformations for dataSource $dataSourceId")

    } match {
      case Success(value) =>
      case Failure(exception) =>
    }
  }

  @throws(classOf[SQLException])
  private def write(dataFrame: DataFrame, fqTableName: String, saveMode: SaveMode, partitionByColumnOpt: Option[String]): Unit = {

    val dataFrameClass: String = classOf[DataFrame].getSimpleName
    val cachedDataFrame: DataFrame = dataFrame.cache()
    if (cachedDataFrame.isEmpty) {
      log.warn(s"Given $dataFrameClass for table $fqTableName is empty. Thus, no data will be written to it")
    } else {

      // Add technical columns
      val now = new Timestamp(System.currentTimeMillis())
      val applicationStartTime = new Timestamp(sparkSession.sparkContext.startTime)
      val dataFrameToWrite: DataFrame = cachedDataFrame
        .withColumn("insert_ts", lit(now))
        .withColumn("insert_dt", lit(timestampToString(now, "yyyy-MM-dd")))
        .withColumn("application_id", lit(sparkSession.sparkContext.applicationId))
        .withColumn("application_name", lit(sparkSession.sparkContext.appName))
        .withColumn("application_start_time", lit(applicationStartTime))
        .withColumn("application_start_date", lit(timestampToString(applicationStartTime, "yyyy-MM-dd")))

      // Use .saveAsTable if the target does not exist
      val tableExists: Boolean = sparkSession.catalog.tableExists(fqTableName)
      if (tableExists) {
        log.info(s"Target table $fqTableName already exists. Thus, matching given $dataFrameClass to it and saving data with saveMode $saveMode using .insertInto")
        dataFrameToWrite.select(sparkSession.table(fqTableName).columns.map(col): _*)
          .write.mode(saveMode)
          .insertInto(fqTableName)
      } else {
        log.warn(s"Target table $fqTableName does not exist now. Thus, creating it now using .saveAsTable")
        val commonWriter: DataFrameWriter[Row] = dataFrameToWrite.write.mode(saveMode).format("parquet")
        val maybePartitionedWriter: DataFrameWriter[Row] = partitionByColumnOpt match {
          case Some(x) => commonWriter.partitionBy(x)
          case None => commonWriter
        }

        maybePartitionedWriter.saveAsTable(fqTableName)
      }

      // Connect to Impala and trigger statement
      val impalaQLStatement: String = if (!tableExists || saveMode == SaveMode.Overwrite) {
        s"INVALIDATE METADATA $fqTableName"
      } else s"REFRESH $fqTableName"

      log.info(s"Successfully saved data into $fqTableName. Issuing following ImpalaQL statement: $impalaQLStatement")
      impalaJDBCConnection.createStatement().execute(impalaQLStatement)
      log.info(s"Successfully issued following ImpalaQL statement: $impalaQLStatement")
    }
  }
}
