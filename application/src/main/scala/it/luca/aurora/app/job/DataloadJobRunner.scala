package it.luca.aurora.app.job

import it.luca.aurora.app.logging.DataloadJobRecord
import it.luca.aurora.app.option.CliArguments
import it.luca.aurora.app.utils.Utils.interpolateString
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.yaml.{ApplicationYaml, DataSource}
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.utils.ObjectDeserializer.{DataFormat, deserializeFile, deserializeString}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.sql.{Connection, DriverManager, SQLException}
import scala.util.{Failure, Success, Try}

class DataloadJobRunner(protected val cliArguments: CliArguments)
  extends Logging {

  def run(): Unit = {

    val dataSourceId: String = cliArguments.dataSource
    val sparkSession: SparkSession = initSparkSession()
    Try {

      // Deserialize application's .yaml file
      val yaml: ApplicationYaml = deserializeFile(new File(cliArguments.yamlFileName), classOf[ApplicationYaml]).withInterpolation()
      val impalaJDBCConnection: Connection = initImpalaJDBCConnection(yaml)
      val dataSource: DataSource = yaml.getDataSourceWithId(dataSourceId)

      // Read metadata file as a single String, interpolate it and deserialize it into a Java object
      val fs: FileSystem = sparkSession.getFileSystem
      val jsonString: String = fs.readFileAsString(new Path(dataSource.getMetadataFilePath))
      val interpolatedJsonString: String = interpolateString(jsonString, yaml)
      log.info(s"Successfully interpolated content of file ${dataSource.getMetadataFilePath}")
      val dataSourceMetadata: DataSourceMetadata = deserializeString(interpolatedJsonString, classOf[DataSourceMetadata], DataFormat.Json)

      // Check files within dataSource's input folder
      val (landingPath, fileNameRegex): (String, String) = (dataSourceMetadata.getDataSourcePaths.getLanding, dataSourceMetadata.getFileNameRegex)
      val validInputFiles: Seq[FileStatus] = fs.getListOfMatchingFiles(new Path(landingPath), fileNameRegex.r)
      if (validInputFiles.nonEmpty) {

        // Initialize job and run it on every valid file
        val dataloadJob = new DataloadJob(sparkSession, impalaJDBCConnection, yaml, dataSource, dataSourceMetadata)
        val dataloadJobRecords: Seq[DataloadJobRecord] = validInputFiles.map(dataloadJob.run)
      } else {
        log.warn(s"Found no input file(s) within path $landingPath matching regex $fileNameRegex")
      }
    } match {
      case Success(_) => log.info(s"Successfully executed ingestion job for dataSource $dataSourceId")
      case Failure(exception) => log.error(s"Caught exception while executing ingestion job for dataSource $dataSourceId. Stack trace: ", exception)
    }
  }

  private def initSparkSession(): SparkSession = {

    val sparkSession = SparkSession.builder
      .enableHiveSupport
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate

    log.info("Successfully initialized {}", classOf[SparkSession].getSimpleName)
    sparkSession
  }

  /**
   * Initializes a [[Connection]] to Impala
   * @param yaml instance of [[ApplicationYaml]]
   * @throws java.lang.ClassNotFoundException if JDBC driver class is not found
   * @throws java.sql.SQLException if connection's initialization fails
   * @return instance of [[Connection]]
   */

  @throws[ClassNotFoundException]
  @throws[SQLException]
  private def initImpalaJDBCConnection(yaml: ApplicationYaml): Connection = {

    val driverClassName: String = yaml.getProperty("impala.jdbc.driverClass")
    val impalaJdbcUrl: String = yaml.getProperty("impala.jdbc.url")

    Class.forName(driverClassName)
    val connection: Connection = DriverManager.getConnection(impalaJdbcUrl)
    log.info("Successfully initialized Impala JDBC connection with URL {}", impalaJdbcUrl)
    connection
  }

  private def writeLogRecords(records: Seq[DataloadJobRecord], sparkSession: SparkSession): Unit = {

    import sparkSession.implicits._

    val partitionColumn: String = records.head.partitionColumn
    val jobRecordsDataFrame: DataFrame = records.toDF().withSqlNamingConvention()
    log.info(s"Successfully converted ${records.size} ${classOf[DataloadJobRecord].getSimpleName}(s) to a ${classOf[DataFrame].getSimpleName}")

  }
}
