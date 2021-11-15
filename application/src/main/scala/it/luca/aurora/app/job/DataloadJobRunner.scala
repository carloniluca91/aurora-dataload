package it.luca.aurora.app.job

import it.luca.aurora.app.option.CliArguments
import it.luca.aurora.configuration.DataSourceWrapper
import it.luca.aurora.configuration.JsonDeserializer.deserializeStringAs
import it.luca.aurora.configuration.datasource.DataSource
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.core.Logging
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.utils._
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.sql.{Connection, DriverManager, SQLException}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object DataloadJobRunner
  extends Logging {

  /**
   * Run ingestion job for given arguments
   * @param cliArguments application's arguments
   */

  def run(cliArguments: CliArguments): Unit = {

    val propertiesFileName: String = cliArguments.propertiesFileName
    val dataSourceId: String = cliArguments.dataSourceId
    Try {

      // Read both .properties file and dataSources .json file. Interpolate the latter
      val properties: PropertiesConfiguration = loadPropertiesFile(propertiesFileName)
      val dataSourcesJsonFile: String = replaceTokensWithProperties(readLocalFileAsString(cliArguments.dataSourcesFileName), properties)
      val dataSourceWrapper: DataSourceWrapper = deserializeStringAs(dataSourcesJsonFile, classOf[DataSourceWrapper])
      val dataSource: DataSource = dataSourceWrapper.getDataSourceWithId(dataSourceId)

      // Read metadata file from HDFS as a single String, interpolate it and deserialize it as Scala object
      val sparkSession: SparkSession = initSparkSession()
      val fs: FileSystem = sparkSession.getFileSystem
      val metadataJsonString: String = replaceTokensWithProperties(fs.readHDFSFileAsString(dataSource.metadataFilePath), properties)
      log.info(s"Successfully interpolated content of file ${dataSource.metadataFilePath}")
      val dataSourceMetadata: DataSourceMetadata = deserializeStringAs(metadataJsonString, classOf[DataSourceMetadata])

      // Check files within dataSource's input folder
      val extract: Extract = dataSourceMetadata.extract
      val (landingPath, fileNameRegex): (String, String) = (extract.landingPath, extract.fileNameRegex)
      val validInputFiles: Seq[FileStatus] = fs.getMatchingFiles(new Path(landingPath), fileNameRegex.r)
      if (validInputFiles.isEmpty) {
        log.warn(s"Found no input file(s) within path $landingPath matching regex $fileNameRegex. So, nothing will be ingested")
      } else {
        val impalaJDBCConnection: Connection = initImpalaJDBCConnection(properties)
        new DataloadJob(sparkSession, impalaJDBCConnection, properties, dataSource, dataSourceMetadata)
          .processFiles(validInputFiles)
      }
    } match {
      case Success(_) => log.info(s"Successfully executed ingestion job for dataSource $dataSourceId")
      case Failure(exception) => log.error(s"Caught exception while executing ingestion job for dataSource $dataSourceId. Stack trace: ", exception)
    }
  }

  /**
   * Read a local file as a single string
   * @param fileName name of local file
   * @return
   */

  protected def readLocalFileAsString(fileName: String): String = {

    val source = Source.fromFile(fileName)
    val output: String = source.mkString("")
    source.close()
    log.info(s"Successfully read file $fileName as a string")
    output
  }

  /**
   * Initialize a [[SparkSession]]
   * @return instance of [[SparkSession]]
   */

  protected def initSparkSession(): SparkSession = {

    val sparkSession = SparkSession.builder
      .enableHiveSupport
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate

    log.info(s"Successfully initialized ${classOf[SparkSession].getSimpleName}")
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
  protected def initImpalaJDBCConnection(properties: PropertiesConfiguration): Connection = {

    val driverClassName: String = properties.getString("impala.jdbc.driverClass")
    val impalaJdbcUrl: String = properties.getString("impala.jdbc.url")

    Class.forName(driverClassName)
    val connection: Connection = DriverManager.getConnection(impalaJdbcUrl)
    log.info("Successfully initialized Impala JDBC connection with URL {}", impalaJdbcUrl)
    connection
  }
}