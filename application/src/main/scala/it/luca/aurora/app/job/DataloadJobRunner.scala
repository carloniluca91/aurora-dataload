package it.luca.aurora.app.job

import it.luca.aurora.app.logging.DataloadJobRecord
import it.luca.aurora.app.option.CliArguments
import it.luca.aurora.app.utils.FSUtils.getValidFilesWithin
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
import scala.io.Source
import scala.util.{Failure, Success, Try}

class DataloadJobRunner(protected val cliArguments: CliArguments)
  extends Logging {

  def run(): Unit = {

    val dataSourceId: String = cliArguments.dataSource
    val sparkSession: SparkSession = initSparkSession()
    Try {

      // Deserialize application's .yaml file
      val yaml: ApplicationYaml = deserializeFile(new File(cliArguments.yamlFileName), classOf[ApplicationYaml]).withInterpolation()
      log.info(s"Successfully read file ${cliArguments.yamlFileName}")
      val impalaJDBCConnection: Connection = initImpalaJDBCConnection(yaml)
      val dataSource: DataSource = yaml.getDataSourceWithId(dataSourceId)

      // Read metadata file as a single String, interpolate it and deserialize it into a Java object
      val fs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val metadataFilePath: Path = new Path(dataSource.getMetadataFilePath)
      val jsonString: String = Source
        .fromInputStream(fs.open(metadataFilePath))
        .getLines().mkString(" ")
      val interpolatedJsonString: String = interpolateString(jsonString, yaml)
      log.info(s"Successfully interpolated content of file $metadataFilePath")
      val dataSourceMetadata: DataSourceMetadata = deserializeString(interpolatedJsonString, classOf[DataSourceMetadata], DataFormat.Json)

      // Check files within dataSource's input folder
      val (landingPath, fileNameRegex): (String, String) = (dataSourceMetadata.getDataSourcePaths.getLanding,
        dataSourceMetadata.getEtlConfiguration.getExtract.getFileNameRegex)
      val validInputFiles: Seq[FileStatus] = getValidFilesWithin(fs, new Path(landingPath), fileNameRegex)
      if (validInputFiles.nonEmpty) {
        val dataloadJob = new DataloadJob(sparkSession, impalaJDBCConnection, yaml, dataSource, dataSourceMetadata)
        val dataloadJobRecords: Seq[DataloadJobRecord] = validInputFiles.map(dataloadJob.run)
      } else {
        log.warn(s"Found no input file within path $landingPath matching regex $fileNameRegex")
      }
    } match {
      case Success(value) => log.info(s"Successfully executed ingestion job for dataSource $dataSourceId")
      case Failure(exception) =>
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
