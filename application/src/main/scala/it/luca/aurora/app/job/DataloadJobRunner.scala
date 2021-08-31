package it.luca.aurora.app.job

import it.luca.aurora.app.logging.DataloadJobRecord
import it.luca.aurora.app.option.CliArguments
import it.luca.aurora.app.utils.Utils.interpolateString
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.yaml.{ApplicationYaml, DataSource}
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.utils.ObjectDeserializer.{DataFormat, deserializeFile, deserializeString}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.sql.{Connection, DriverManager, SQLException}
import scala.io.Source

class DataloadJobRunner(protected val cliArguments: CliArguments)
  extends Logging {

  def run(): Unit = {

    val sparkSession: SparkSession = initSparkSession()
    val yaml: ApplicationYaml = deserializeFile(new File(cliArguments.yamlFileName), classOf[ApplicationYaml]).withInterpolation()
    log.info(s"Successfully loaded file ${cliArguments.yamlFileName}")
    val impalaJDBCConnection: Connection = initImpalaJDBCConnection(yaml)
    val dataSource: DataSource = yaml.getDataSourceWithId(cliArguments.dataSource)

    val fs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val metadataFilePath: Path = new Path(dataSource.getMetadataFilePath)
    if (!fs.exists(metadataFilePath)) {
      throw new UnExistingMetadataFileException(dataSource)
    }

    val metadataJsonString: String = Source
      .fromInputStream(fs.open(metadataFilePath))
      .getLines().mkString(" ")

    val metadataJsonStringWithInterpolation: String = interpolateString(metadataJsonString, yaml)
    log.info(s"Successfully interpolated content of file $metadataFilePath")
    val dataSourceMetadata: DataSourceMetadata = deserializeString(metadataJsonStringWithInterpolation, classOf[DataSourceMetadata], DataFormat.Json)
    val dataSourceLandingPath: String = dataSourceMetadata.getDataSourcePaths.getLanding
    val fileStatuses: Seq[FileStatus] = fs.listStatus(new Path(dataSourceLandingPath))
    val isValidInputFile: FileStatus => Boolean =
      f => f.isFile && f.getPath.getName.matches(dataSourceMetadata.getEtlConfiguration.getExtract.getFileNameRegex)
    val invalidInputPaths: Seq[FileStatus] = fileStatuses.filterNot { isValidInputFile }
    if (invalidInputPaths.nonEmpty) {

      val fileOrDirectory: FileStatus => String = x => if (x.isDirectory) "directory" else "file"
      val invalidInputPathsStr = s"${invalidInputPaths.map { x => s"  Name: ${x.getPath.getName} (${fileOrDirectory(x)}})" }.mkString("\n") }"
      log.warn(s"Found ${invalidInputPaths.size} invalid file(s) (or directories) at path $dataSourceLandingPath.\n$invalidInputPathsStr")
    }

    val validInputFiles: Seq[FileStatus] = fileStatuses.filter { isValidInputFile }
    val dataloadJob = new DataloadJob(sparkSession, impalaJDBCConnection, yaml, dataSource, dataSourceMetadata)
    val dataloadJobRecords: Seq[DataloadJobRecord] = validInputFiles.map(dataloadJob.run)
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
