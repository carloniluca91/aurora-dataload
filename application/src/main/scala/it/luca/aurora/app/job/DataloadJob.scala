package it.luca.aurora.app.job

import it.luca.aurora.app.logging.DataloadJobRecord
import it.luca.aurora.configuration.metadata.{ColumnNameStrategy, DataSourceMetadata, EtlConfiguration, FileNameRegexStrategy}
import it.luca.aurora.configuration.yaml.DataSource
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.job.SparkJob
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.functions.{concat_ws, lit, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Connection
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DataloadJob(override protected val sparkSession: SparkSession,
                  override protected val impalaJDBCConnection: Connection,
                  protected val dataSource: DataSource,
                  protected val dataSourceMetadata: DataSourceMetadata)
  extends SparkJob(sparkSession, impalaJDBCConnection)
    with Logging {

  def run(fileStatus: FileStatus): DataloadJobRecord = {

    val filePath: Path = fileStatus.getPath
    val dataloadRecordFunction: Option[Throwable] => DataloadJobRecord =
      DataloadJobRecord(sparkSession.sparkContext, dataSource, filePath, "x", _: Option[Throwable])

    Try {

      val dataSourceId: String = dataSourceMetadata.getId
      val etlConfiguration : EtlConfiguration = dataSourceMetadata.getEtlConfiguration
      val inputDataFrame: DataFrame = etlConfiguration.getExtract.read(sparkSession, filePath)

      val filterExpressions: Seq[String] = etlConfiguration.getTransform.getFilters
      val filterStatementsAndCols: Seq[(String, Column)] = filterExpressions.map { x => (x, SqlExpressionParser.parse(x)) }
      log.info(s"Successfully parsed all of ${filterExpressions.size} filter(s) for dataSource $dataSourceId")

      val overallFilterCol: Column = filterStatementsAndCols.map{ _._2 }.reduce(_ && _)
      val filterFailureReportCols: Seq[Column] = filterStatementsAndCols.map { x => when(x._2, x._1) }

      // Invalid records (i.e. that do not satisfy all of dataSource filters)
      val invalidRecordsDataFrame: DataFrame = inputDataFrame
        .filter(!overallFilterCol)
        .withColumn("failed_checks", concat_ws(", ", filterFailureReportCols: _*))
        .withInputFilePathCol(filePath)
        .withTechnicalColumns()

      // Valid records
      val transformationExpressions: Seq[String] = etlConfiguration.getTransform.getTransformations
      val trustedDataFrameColumns: Seq[Column] = transformationExpressions.map { SqlExpressionParser.parse }
      log.info(s"Successfully converted all of ${transformationExpressions.size} trasformation(s) for dataSource $dataSourceId")

      val validRecordsDataFrame: DataFrame = inputDataFrame
        .filter(overallFilterCol)
        .select(trustedDataFrameColumns: _*)
        .withInputFilePathCol(filePath)
        .withTechnicalColumns()

      log.info(s"Successfully added all of ${transformationExpressions.size} for dataSource $dataSourceId")

      // Partitioning
      val partitionCol: Column = dataSourceMetadata.getPartitionStrategy match {
        case regex: FileNameRegexStrategy => lit(regex.getDateFromFileName(dataSourceMetadata.getFileNameRegex, filePath))
        case columnName: ColumnNameStrategy => lit(columnName.getColumnName)
      }
    } match {
      case Success(_) => dataloadRecordFunction(None)
      case Failure(exception) => dataloadRecordFunction(Some(exception))
    }
  }
}
