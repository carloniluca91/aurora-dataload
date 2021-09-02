package it.luca.aurora.app.job

import it.luca.aurora.app.logging.DataloadJobRecord
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
                  protected val yaml: ApplicationYaml,
                  protected val dataSource: DataSource,
                  protected val dataSourceMetadata: DataSourceMetadata)
  extends SparkJob(sparkSession, impalaJDBCConnection)
    with Logging {

  protected final val dataSourceId: String = dataSourceMetadata.getId
  protected final val yarnUiUrl: String = yaml.getProperty("yarn.ui.url")

  def run(fileStatuses: Seq[FileStatus]): Unit = {

    val landingPath: String = dataSourceMetadata.getDataSourcePaths.getLanding
    val fileNameRegex: String = dataSourceMetadata.getFileNameRegex
    if (fileStatuses.isEmpty) {
      log.warn(s"Found no input file(s) within path $landingPath matching regex $fileNameRegex. So, nothing will be ingested")
    } else {

      log.info(s"Found ${fileStatuses.size} file(s) to ingest for dataSource $dataSourceId")
      val fs: FileSystem = sparkSession.getFileSystem
      val dataSourcePaths: DataSourcePaths = dataSourceMetadata.getDataSourcePaths
      val dataloadJobRecords: Seq[DataloadJobRecord] = fileStatuses.map { fileStatus =>

        // Depending on job execution, set optional exception and target directory where file will be moved
        val (exceptionOpt, targetDirectoryPath): (Option[Throwable], Path) = run(fileStatus) match {
          case Success(_) => (None, dataSourcePaths.getSuccessPath)
          case Failure(exception) => (Some(exception), dataSourcePaths.getErrorPath)
        }

        // Move file to target directory
        fs.moveFileToDirectory(fileStatus.getPath, targetDirectoryPath)
        buildDataloadJobRecord(fileStatus.getPath, exceptionOpt)
      }

      writeDataloadJobRecords(dataloadJobRecords)
    }
  }

  protected def run(fileStatus: FileStatus): Try[Unit] = {

    Try {

      val filePath: Path = fileStatus.getPath
      val etlConfiguration: EtlConfiguration = dataSourceMetadata.getEtlConfiguration
      val (extract, transform, load): (Extract, Transform, Load) = (etlConfiguration.getExtract, etlConfiguration.getTransform, etlConfiguration.getLoad)
      log.info(s"Starting to ingest file $filePath for dataSource $dataSourceId")

      val inputDataFrame: DataFrame = extract.read(sparkSession, filePath)
      val filterStatementsAndCols: Seq[(String, Column)] = transform.getFilters.map { x => (x, SqlExpressionParser.parse(x)) }
      log.info(s"Successfully parsed all of ${filterStatementsAndCols.size} filter(s) for dataSource $dataSourceId")

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
      val trustedDataFrameColumns: Seq[Column] = transform.getTransformations.map { SqlExpressionParser.parse }
      log.info(s"Successfully converted all of ${trustedDataFrameColumns.size} trasformation(s) for dataSource $dataSourceId")
      val validRecordsDataFrame: DataFrame = inputDataFrame.filter(overallFilterCol)
        .select(trustedDataFrameColumns: _*)
        .withInputFilePathCol(filePath)
        .withTechnicalColumns()
        .withColumn(partitionColumnName, partitionCol)

      log.info(s"Successfully added all of ${trustedDataFrameColumns.size} for dataSource $dataSourceId")

      // Write data
      val (trustedTable, errorTable): (String, String) = (load.getTarget.getTrusted, load.getTarget.getError)
      write(validRecordsDataFrame, trustedTable, SaveMode.Append, Some(partitionColumnName))
      write(invalidRecordsDataFrame, errorTable, SaveMode.Append, Some(partitionColumnName))
      log.info(s"Successfully ingested file ${filePath.toString}")
    }
  }

  protected final def buildDataloadJobRecord(filePath: Path, exceptionOpt: Option[Throwable]): DataloadJobRecord = {

    DataloadJobRecord(sparkContext = sparkSession.sparkContext,
      dataSource = dataSource,
      yarnUiUrl = yarnUiUrl,
      filePath = filePath,
      exceptionOpt = exceptionOpt)
  }

  protected def writeDataloadJobRecords(records: Seq[DataloadJobRecord]): Unit = {

    import sparkSession.implicits._

    val recordsSize: Int = records.size
    val recordClassName: String = classOf[DataloadJobRecord].getSimpleName
    Try {

      val jobRecordsDataFrame: DataFrame = records.toDF().withSqlNamingConvention()
      log.info(s"Successfully converted $recordsSize $recordClassName(s) to a ${classOf[DataFrame].getSimpleName}")
      val targetTable: String = yaml.getProperty("spark.log.table.name")
      val targetTablePartitionColumn: String = yaml.getProperty("spark.log.table.partitionColumn")
      write(jobRecordsDataFrame, targetTable, SaveMode.Append, Some(targetTablePartitionColumn))
    } match {
      case Success(_) => log.info(s"Successfully saved all of $recordsSize $recordClassName(s)")
      case Failure(exception) => log.error(s"Caught exception while saving $records $recordClassName(s). Stack trace: ", exception)
    }
  }
}
