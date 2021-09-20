package it.luca.aurora.app.job

import it.luca.aurora.app.logging.DataloadJobRecord
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.load.{ColumnExpressionInfo, FileNameRegexInfo, Load, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform
import it.luca.aurora.configuration.metadata.{DataSourceMetadata, DataSourcePaths, EtlConfiguration}
import it.luca.aurora.configuration.yaml.DataSource
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import it.luca.aurora.core.{Logging, SparkJob}
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.functions.{concat_ws, lit, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Connection
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DataloadJob(override protected val sparkSession: SparkSession,
                  override protected val impalaJDBCConnection: Connection,
                  protected val properties: PropertiesConfiguration,
                  protected val dataSource: DataSource,
                  protected val dataSourceMetadata: DataSourceMetadata)
  extends SparkJob(sparkSession, impalaJDBCConnection)
    with Logging {

  protected final val yarnUiUrl: String = properties.getString("yarn.ui.url")
  protected final val fsPermission: FsPermission = FsPermission.valueOf(properties.getString("hdfs.defaultPermissions"))

  /**
   * Run ingestion job for each valid input file
   * @param inputFiles valid [[FileStatus]](es)
   */

  def processFiles(inputFiles: Seq[FileStatus]): Unit = {

    val landingPath: String = dataSourceMetadata.getDataSourcePaths.getLanding
    val fileNameRegex: String = dataSourceMetadata.getFileNameRegex
    if (inputFiles.isEmpty) {
      log.warn(s"Found no input file(s) within path $landingPath matching regex $fileNameRegex. So, nothing will be ingested")
    } else {

      log.info(s"Found ${inputFiles.size} file(s) to be ingested (${inputFiles.map(_.getPath).mkString("|")})")
      val fs: FileSystem = sparkSession.getFileSystem
      val dataSourcePaths: DataSourcePaths = dataSourceMetadata.getDataSourcePaths
      val dataloadJobRecords: Seq[DataloadJobRecord] = inputFiles.map { inputFile =>

        // Depending on job execution, set optional exception and target directory where file will be moved
        val (exceptionOpt, targetDirectoryPath): (Option[Throwable], String) = processFile(inputFile) match {
          case Success(_) => (None, dataSourcePaths.getSuccess)
          case Failure(exception) =>

            log.error(s"Caught exception while ingesting file ${inputFile.getPath}. Stack trace: ", exception)
            (Some(exception), dataSourcePaths.getError)
        }

        val filePath: Path = inputFile.getPath
        fs.moveFileToDirectory(filePath, new Path(targetDirectoryPath), fsPermission)
        buildDataloadJobRecord(filePath, exceptionOpt)
      }

      writeDataloadJobRecords(dataloadJobRecords)
    }
  }

  /**
   * Run ingestion job for a valid input file
   * @param fileStatus valid [[FileStatus]]
   * @return instance of [[Try]] (i.e. a [[Success]] if ingestion job succeeded, a [[Failure]] instead)
   */

  protected def processFile(fileStatus: FileStatus): Try[Unit] = {

    Try {

      val filePath: Path = fileStatus.getPath
      val etlConfiguration: EtlConfiguration = dataSourceMetadata.getEtlConfiguration
      val (extract, transform, load): (Extract, Transform, Load) = (etlConfiguration.getExtract, etlConfiguration.getTransform, etlConfiguration.getLoad)
      log.info(s"Starting to ingest file $filePath")

      // Read file
      val inputDataFrame: DataFrame = extract.read(sparkSession, filePath)
      val filterStatementsAndCols: Seq[(String, Column)] = transform.getFilters.map { x => (x, SqlExpressionParser.parse(x)) }
      log.info(s"Successfully parsed all of ${filterStatementsAndCols.size} filter(s)")
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
      val errorDf: DataFrame = inputDataFrame.filter(!overallFilterCol)
        .withColumn("failed_checks_report", concat_ws(", ", filterFailureReportCols: _*))
        .withInputFilePathCol(filePath)
        .withTechnicalColumns()
        .withColumn(partitionColumnName, partitionCol)

      // Valid records
      val trustedDfColumns: Seq[Column] = transform.getTransformations.map { SqlExpressionParser.parse }
      log.info(s"Successfully converted all of ${trustedDfColumns.size} trasformation(s)")
      val nonFinalTrustedDf: DataFrame = inputDataFrame.filter(overallFilterCol)
        .select(trustedDfColumns: _*)
        .withInputFilePathCol(filePath)
        .withTechnicalColumns()
        .withColumn(partitionColumnName, partitionCol)

      log.info(s"Successfully added all of ${trustedDfColumns.size}")

      // Optionally remove duplicates and drop columns
      val finalTrustedDf: DataFrame = transform.maybeDropDuplicatesAndColumns(nonFinalTrustedDf)

      // Write data
      val (trustedTable, errorTable): (String, String) = (load.getTarget.getTrusted, load.getTarget.getError)
      saveAsOrInsertInto(finalTrustedDf, trustedTable, partitionColumnName)
      saveAsOrInsertInto(errorDf, errorTable, partitionColumnName)
      log.info(s"Successfully ingested file $filePath")
    }
  }

  /**
   * Create a [[DataloadJobRecord]] for given [[Path]] and potential exception raised by ingestion job
   * @param filePath [[Path]] of ingested file
   * @param exceptionOpt potential exception raised by ingestion job
   * @return [[DataloadJobRecord]]
   */

  protected final def buildDataloadJobRecord(filePath: Path, exceptionOpt: Option[Throwable]): DataloadJobRecord = {

    DataloadJobRecord(sparkContext = sparkSession.sparkContext,
      dataSource = dataSource,
      yarnUiUrl = yarnUiUrl,
      filePath = filePath,
      exceptionOpt = exceptionOpt)
  }

  /**
   * Save some [[DataloadJobRecord]]
   * @param records instances of [[DataloadJobRecord]]
   */

  protected def writeDataloadJobRecords(records: Seq[DataloadJobRecord]): Unit = {

    import sparkSession.implicits._

    val recordsDescription: String = s"${records.size} ${classOf[DataloadJobRecord].getSimpleName}(s)"
    Try {

      val jobRecordsDataFrame: DataFrame = records.toDF().withSqlNamingConvention()
      log.info(s"Successfully converted $recordsDescription to a ${classOf[DataFrame].getSimpleName}")
      val targetTable: String = properties.getString("spark.log.table.name")
      val partitionColumn: String = properties.getString("spark.log.table.partitionColumn")
      saveAsOrInsertInto(jobRecordsDataFrame, targetTable, partitionColumn)
    } match {
      case Success(_) => log.info(s"Successfully saved all of $recordsDescription")
      case Failure(exception) => log.warn(s"Caught exception while saving $recordsDescription. Stack trace: ", exception)
    }
  }
}
