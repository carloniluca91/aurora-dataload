package it.luca.aurora.app.job

import it.luca.aurora.app.option.CliArguments
import it.luca.aurora.app.utils.loadProperties
import it.luca.aurora.configuration.ObjectDeserializer.{deserializeFile, deserializeString}
import it.luca.aurora.configuration.datasource.{DataSource, DataSourcesWrapper}
import it.luca.aurora.configuration.implicits._
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.core.Logging
import it.luca.aurora.core.implicits._
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.sql.{Connection, DriverManager, SQLException}
import scala.util.{Failure, Success, Try}

class DataloadJobRunner
  extends Logging {

  /**
   * Setup and run ingestion job for given arguments
   * @param cliArguments instance of [[CliArguments]]
   */

  def run(cliArguments: CliArguments): Unit = {

    val propertiesFileName: String = cliArguments.propertiesFileName
    val dataSourceId: String = cliArguments.dataSourceId
    Try {

      // Deserialize both .properties and dataSources .json file
      val properties: PropertiesConfiguration = loadProperties(propertiesFileName)
      val impalaJDBCConnection: Connection = initImpalaJDBCConnection(properties)
      val wrapper: DataSourcesWrapper = deserializeFile(new File(cliArguments.dataSourcesFileName), classOf[DataSourcesWrapper])
      val dataSource: DataSource = wrapper.getDataSourceWithId(dataSourceId).withInterpolation(properties)

      // Read metadata file as a single String, interpolate it and deserialize it into a Java object
      val sparkSession: SparkSession = initSparkSession()
      val fs: FileSystem = sparkSession.getFileSystem
      val jsonString: String = fs.readFileAsString(dataSource.getMetadataFilePath).withInterpolation(properties)
      log.info(s"Successfully interpolated content of file ${dataSource.getMetadataFilePath}")
      val dataSourceMetadata: DataSourceMetadata = deserializeString(jsonString, classOf[DataSourceMetadata])

      // Check files within dataSource's input folder
      val (landingPath, fileNameRegex): (String, String) = (dataSourceMetadata.getDataSourcePaths.getLanding, dataSourceMetadata.getFileNameRegex)
      val validInputFiles: Seq[FileStatus] = fs.getListOfMatchingFiles(new Path(landingPath), fileNameRegex.r)
      new DataloadJob(sparkSession, impalaJDBCConnection, properties, dataSource, dataSourceMetadata)
        .processFiles(validInputFiles)
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
   * @param properties instance of [[PropertiesConfiguration]]
   * @throws java.lang.ClassNotFoundException if JDBC driver class is not found
   * @throws java.sql.SQLException if connection's initialization fails
   * @return instance of [[Connection]]
   */

  @throws[ClassNotFoundException]
  @throws[SQLException]
  private def initImpalaJDBCConnection(properties: PropertiesConfiguration): Connection = {

    val driverClassName: String = properties.getString("impala.jdbc.driverClass")
    val impalaJdbcUrl: String = properties.getString("impala.jdbc.url")

    Class.forName(driverClassName)
    val connection: Connection = DriverManager.getConnection(impalaJdbcUrl)
    log.info("Successfully initialized Impala JDBC connection with URL {}", impalaJdbcUrl)
    connection
  }
}