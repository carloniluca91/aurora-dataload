package it.luca.aurora.app.job

import it.luca.aurora.app.logging.DataloadJobRecord
import it.luca.aurora.app.utils.FSUtils.{getFileSystem, moveFileToDirectory}
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.load.{ColumnExpressionInfo, FileNameRegexInfo, Load, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform
import it.luca.aurora.configuration.metadata.{DataSourceMetadata, DataSourcePaths, EtlConfiguration}
import it.luca.aurora.configuration.yaml.{ApplicationYaml, DataSource}
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.job.SparkJob
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.functions.{concat_ws, lit, when}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import java.sql.Connection
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DataloadJob(override protected val sparkSession: SparkSession,
                  override protected val impalaJDBCConnection: Connection,
                  protected val applicationYaml: ApplicationYaml,
                  protected val dataSource: DataSource,
                  protected val dataSourceMetadata: DataSourceMetadata)
  extends SparkJob(sparkSession, impalaJDBCConnection)
    with Logging {

  protected final def buildJobRecord(filePath: Path, exceptionOpt: Option[Throwable]): DataloadJobRecord = {

    DataloadJobRecord(sparkContext = sparkSession.sparkContext,
      dataSource = dataSource,
      yarnUiUrl = applicationYaml.getProperty("yarn.ui.url"),
      filePath = filePath,
      exceptionOpt = exceptionOpt)
  }

  def run(fileStatus: FileStatus): DataloadJobRecord = {

    val filePath: Path = fileStatus.getPath
    val fs: FileSystem = getFileSystem(sparkSession)
    val (dataSourceId, etlConfiguration): (String, EtlConfiguration) = (dataSourceMetadata.getId, dataSourceMetadata.getEtlConfiguration)
    val (extract, transform, load): (Extract, Transform, Load) = (etlConfiguration.getExtract, etlConfiguration.getTransform, etlConfiguration.getLoad)
    val dataSourcePaths: DataSourcePaths = dataSourceMetadata.getDataSourcePaths
    Try {

      val inputDataFrame: DataFrame = extract.read(sparkSession, filePath)
      val filterExpressions: Seq[String] = transform.getFilters
      val filterStatementsAndCols: Seq[(String, Column)] = filterExpressions.map { x => (x, SqlExpressionParser.parse(x)) }
      log.info(s"Successfully parsed all of ${filterExpressions.size} filter(s) for dataSource $dataSourceId")

      val overallFilterCol: Column = filterStatementsAndCols.map{ _._2 }.reduce(_ && _)
      val filterFailureReportCols: Seq[Column] = filterStatementsAndCols.map { x => when(x._2, x._1) }

      // Partitioning
      val partitionInfo: PartitionInfo = load.getPartitionInfo
      val partitionColumnName: String = partitionInfo.getColumnName
      val partitionCol: Column = partitionInfo match {
        case r: FileNameRegexInfo => lit(r.getDateFromFileName(extract.getFileNameRegex, filePath))
        case c: ColumnExpressionInfo =>
          val column: Column = SqlExpressionParser.parse(c.getColumnExpression)
          log.info(s"Successfully parsed partitioning expression from ${classOf[ColumnExpressionInfo].getSimpleName}")
          column
      }

      // Invalid records (i.e. that do not satisfy all of dataSource filters)
      val invalidRecordsDataFrame: DataFrame = inputDataFrame.filter(!overallFilterCol)
        .withColumn("failed_checks", concat_ws(", ", filterFailureReportCols: _*))
        .withInputFilePathCol(filePath)
        .withTechnicalColumns()
        .withColumn(partitionColumnName, partitionCol)

      // Valid records
      val transformationExpressions: Seq[String] = transform.getTransformations
      val trustedDataFrameColumns: Seq[Column] = transformationExpressions.map { SqlExpressionParser.parse }
      log.info(s"Successfully converted all of ${transformationExpressions.size} trasformation(s) for dataSource $dataSourceId")

      val validRecordsDataFrame: DataFrame = inputDataFrame.filter(overallFilterCol)
        .select(trustedDataFrameColumns: _*)
        .withInputFilePathCol(filePath)
        .withTechnicalColumns()
        .withColumn(partitionColumnName, partitionCol)

      log.info(s"Successfully added all of ${transformationExpressions.size} for dataSource $dataSourceId")

      val (trustedTable, errorTable): (String, String) = (load.getTarget.getTrusted, load.getTarget.getError)
      write(validRecordsDataFrame, trustedTable, SaveMode.Append, Some(partitionColumnName))
      write(invalidRecordsDataFrame, errorTable, SaveMode.Append, Some(partitionColumnName))

      log.info(s"Successfully ingested file ${filePath.toString}")
    } match {
      case Success(_) =>
        moveFileToDirectory(fs, filePath, dataSourcePaths.getSuccessPath)
        buildJobRecord(filePath, None)
      case Failure(exception) =>
        moveFileToDirectory(fs, filePath, dataSourcePaths.getErrorPath)
        buildJobRecord(filePath, Some(exception))
    }
  }
}
