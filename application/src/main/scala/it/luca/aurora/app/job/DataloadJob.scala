package it.luca.aurora.app.job

import it.luca.aurora.configuration.datasource.DataSource
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.load.{Load, StagingPaths}
import it.luca.aurora.configuration.metadata.transform.{ColumnPartitioning, FileNameRegexPartitioning, Transform}
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import it.luca.aurora.core.{Logging, SparkJob}
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.functions.{concat_ws, lit, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Connection
import scala.util.{Failure, Success, Try}

class DataloadJob(override protected val sparkSession: SparkSession,
                  override protected val impalaJDBCConnection: Connection,
                  protected val properties: PropertiesConfiguration,
                  protected val dataSource: DataSource,
                  protected val dataSourceMetadata: DataSourceMetadata)
  extends SparkJob(sparkSession, impalaJDBCConnection)
    with Logging {

  protected final val fs: FileSystem = sparkSession.getFileSystem
  protected final val hadoopUserName: String = properties.getString("hadoop.user.name")
  protected final val targetDirectoryPermissions: FsPermission = FsPermission.valueOf(properties.getString("hadoop.target.directory.permissions"))
  protected final val tablePermissions: FsPermission = FsPermission.valueOf(properties.getString("spark.output.table.permissions"))
  protected final val maxFileSizeInBytes: Int = properties.getInt("spark.output.file.maxSizeInBytes")
  protected final val yarnUiUrl: String = properties.getString("yarn.ui.url")

  /**
   * Run ingestion job for each valid input file
   * @param inputFiles valid [[FileStatus]](es)
   */

  def processFiles(inputFiles: Seq[FileStatus]): Unit = {

    val filesToIngestStr: String = inputFiles.map { x => s"  ${x.getPath.getName}" }.mkString("\n")
    log.info(s"Found ${inputFiles.size} file(s) to be ingested\n\n$filesToIngestStr\n")
    val dataloadJobLogRecords: Seq[DataloadJobLogRecord] = inputFiles.map { inputFile =>

      // Depending on job execution, set optional exception and target directory where processed file will be moved
      val staginsPaths: StagingPaths = dataSourceMetadata.load.stagingPaths
      val (exceptionOpt, targetDirectoryPath): (Option[Throwable], String) = processFile(inputFile) match {
        case Success(_) => (None, staginsPaths.success)
        case Failure(exception) =>
          log.error(s"Caught exception while ingesting file ${inputFile.getPath.getName}. Stack trace: ", exception)
          (Some(exception), staginsPaths.failed)
      }

      val filePath: Path = inputFile.getPath
      fs.moveFileToDirectory(filePath, new Path(targetDirectoryPath), targetDirectoryPermissions)
      DataloadJobLogRecord(sparkSession, dataSource, yarnUiUrl, filePath, exceptionOpt)
    }

    writeJobLogRecords(dataloadJobLogRecords)
  }

  /**
   * Run ingestion job for a valid input file
   * @param fileStatus valid [[FileStatus]]
   * @return instance of [[Try]] (i.e. a [[Success]] if ingestion job succeeded, a [[Failure]] instead)
   */

  protected def processFile(fileStatus: FileStatus): Try[Unit] = {

    Try {

      val filePath: Path = fileStatus.getPath
      val extract: Extract = dataSourceMetadata.extract
      val (transform, load): (Transform, Load) = (dataSourceMetadata.transform, dataSourceMetadata.load)
      log.info(s"Starting to ingest file ${filePath.getName}")

      // Read file
      val inputDataFrame: DataFrame = extract.read(sparkSession, filePath)
      val filterStatementsAndCols: Seq[(String, Column)] = transform.filters.map { x => (x, SqlExpressionParser.parse(x)) }
      log.info(s"Successfully parsed all of ${filterStatementsAndCols.size} filter(s)")
      val overallFilterCol: Column = filterStatementsAndCols.map { case (_, column) => column }.reduce(_ && _)
      val filterFailureReportCols: Seq[Column] = filterStatementsAndCols.map { case (string, column) => when(!column, string) }

      // Partitioning
      val partitionColumnName: String = transform.partitioning.columnName
      val partitionCol: Column = transform.partitioning match {
        case f: FileNameRegexPartitioning => lit(f.getDateFromFileName(extract.fileNameRegex.r, filePath.getName))
        case c: ColumnPartitioning =>
          val column: Column = SqlExpressionParser.parse(c.columnExpression)
          log.info(s"Successfully parsed partitioning expression from ${classOf[ColumnPartitioning].getSimpleName}")
          column
      }

      // Invalid records (i.e. that do not satisfy all of dataSource filters)
      val failedChecksReportColumnName: String = properties.getString("spark.column.failedChecksReport")
      val inputFilePathColumnName: String = properties.getString("spark.column.inputFilePath")
      val errorDf: DataFrame = inputDataFrame.filter(!overallFilterCol)
        .withColumn(failedChecksReportColumnName, concat_ws(", ", filterFailureReportCols: _*))
        .withInputFilePathCol(inputFilePathColumnName, filePath)
        .withTechnicalColumns()
        .withColumn(partitionColumnName, partitionCol)

      // Valid records
      val trustedDfColumns: Seq[Column] = transform.transformations.map { SqlExpressionParser.parse }
      log.info(s"Successfully parsed all of ${trustedDfColumns.size} trasformation(s)")
      val nonFinalTrustedDf: DataFrame = inputDataFrame.filter(overallFilterCol)
        .select(trustedDfColumns: _*)
        .withInputFilePathCol(inputFilePathColumnName, filePath)
        .withTechnicalColumns()
        .withColumn(partitionColumnName, partitionCol)

      log.info(s"Successfully applied all of ${trustedDfColumns.size} transformation(s)")

      // Optionally remove duplicates and drop columns
      val finalTrustedDf: DataFrame = transform.maybeDropDuplicatesAndColumns(nonFinalTrustedDf)

      // Write data
      val (trustedTableName, errorTableName): (String, String) = (load.stagingTables.trusted, load.stagingTables.error)
      saveIfNotEmpty(finalTrustedDf, trustedTableName, partitionColumnName)
      saveIfNotEmpty(errorDf, errorTableName, partitionColumnName)
      log.info(s"Successfully ingested file ${filePath.getName}")
    }
  }

  /**
   * Save log records produced by the ingestion job
   * @param records [[Seq]] of [[DataloadJobLogRecord]]
   */

  protected def writeJobLogRecords(records: Seq[DataloadJobLogRecord]): Unit = {

    import sparkSession.implicits._

    val recordsDescription: String = s"${records.size} ${classOf[DataloadJobLogRecord].getSimpleName}(s)"
    Try {

      val jobRecordsDataFrame: DataFrame = records.toDF()
        .withSqlNamingConvention()
        .coalesce(1)

      log.info(s"Successfully converted $recordsDescription to a ${classOf[DataFrame].getSimpleName}")
      val targetTable: String = properties.getString("spark.log.table.name")
      val partitionColumn: String = properties.getString("spark.log.table.partitionColumn")
      save(jobRecordsDataFrame, targetTable, partitionColumn)
    } match {
      case Success(_) => log.info(s"Successfully saved all of $recordsDescription")
      case Failure(exception) => log.warn(s"Caught exception while saving $recordsDescription. Stack trace: ", exception)
    }
  }

  /**
   * If given dataframe is not empty, save it to given table using given partitioning column
   * @param dataFrame [[DataFrame]]
   * @param fqTableName fully qualified (i.e. db.table) name of target table
   * @param partitionColumn partitioning column
   */

  protected def saveIfNotEmpty(dataFrame: DataFrame, fqTableName: String, partitionColumn: String): Unit = {

    if (dataFrame.isEmpty) {
      log.warn(s"Given ${classOf[DataFrame].getSimpleName} for target table $fqTableName is empty. Thus, no data will be written to it")
    } else {
      save(dataFrame.withOptimizedRepartitioning(maxFileSizeInBytes), fqTableName, partitionColumn)
    }
  }

  /**
   * Save a dataframe to a Hive table using given partitioning column
   * @param dataFrame [[DataFrame]] to be saved
   * @param fqTargetTable fully qualified (i.e. db.table) name of target table
   * @param partitionColumn partitioning column
   */

  protected def save(dataFrame: DataFrame, fqTargetTable: String, partitionColumn: String): Unit = {

    val isNewTable: Boolean = saveAsOrInsertInto(dataFrame, fqTargetTable, partitionColumn)
    val tableLocation: String = sparkSession.getTableLocation(fqTargetTable)
    val anyChangeToPartitions: Boolean = fs.modifyTablePermissions(tableLocation, hadoopUserName, tablePermissions)
    executeImpalaStatement(fqTargetTable, isNewTable || anyChangeToPartitions)
  }
}
