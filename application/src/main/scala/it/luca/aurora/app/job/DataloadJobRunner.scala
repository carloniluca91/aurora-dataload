package it.luca.aurora.app.job

import it.luca.aurora.app.option.CliArguments
import it.luca.aurora.app.utils.Utils.interpolateString
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.yaml.{ApplicationYaml, DataSource}
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.utils.ObjectDeserializer.{DataFormat, deserializeFile, deserializeString}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.sql.{Connection, DriverManager, SQLException}
import scala.util.{Failure, Success, Try}

class DataloadJobRunner(protected val cliArguments: CliArguments)
  extends Logging {

  protected final val dataSourceId: String = cliArguments.dataSource

  def run(): Unit = {

    Try {

      // Deserialize application's .yaml file
      val yaml: ApplicationYaml = deserializeFile(new File(cliArguments.yamlFileName), classOf[ApplicationYaml]).withInterpolation()
      val impalaJDBCConnection: Connection = initImpalaJDBCConnection(yaml)
      val dataSource: DataSource = yaml.getDataSourceWithId(dataSourceId)

      // Read metadata file as a single String, interpolate it and deserialize it into a Java object
      val sparkSession: SparkSession = initSparkSession()
      val fs: FileSystem = sparkSession.getFileSystem
      val jsonString: String = fs.readFileAsString(new Path(dataSource.getMetadataFilePath))
      val interpolatedJsonString: String = interpolateString(jsonString, yaml)
      log.info(s"Successfully interpolated content of file ${dataSource.getMetadataFilePath}")
      val dataSourceMetadata: DataSourceMetadata = deserializeString(interpolatedJsonString, classOf[DataSourceMetadata], DataFormat.Json)

      // Check files within dataSource's input folder
      val (landingPath, fileNameRegex): (String, String) = (dataSourceMetadata.getDataSourcePaths.getLanding, dataSourceMetadata.getFileNameRegex)
      val validInputFiles: Seq[FileStatus] = fs.getListOfMatchingFiles(new Path(landingPath), fileNameRegex.r)
      new DataloadJob(sparkSession, impalaJDBCConnection, yaml, dataSource, dataSourceMetadata)
        .run(validInputFiles)
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
}
