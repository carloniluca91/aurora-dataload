package it.luca.aurora

import it.luca.aurora.core.configuration.yaml.{ApplicationYaml, DataSource}
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.utils.ObjectDeserializer
import it.luca.aurora.option.CliArguments
import org.apache.spark.sql.SparkSession

import java.io.File
import java.sql.{Connection, DriverManager, SQLException}

class DataLoadJob(protected val cliArguments: CliArguments)
  extends Logging {

  @throws(classOf[Exception])
  def run(): Unit = {

    val sparkSession: SparkSession = initSparkSession()
    val yaml: ApplicationYaml = ObjectDeserializer.deserialize(new File(cliArguments.yamlFileName), classOf[ApplicationYaml])
      .withInterpolation()
    log.info(s"Successfully loaded file ${cliArguments.yamlFileName}")
    val impalaJDBCConnection: Connection = initImpalaJDBCConnection(yaml)
    val dataSource: DataSource = yaml.getDataSourceWithId(cliArguments.dataSource)

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
   * Initializes a JDBC [[java.sql.Connection]] to Impala
 *
   * @param yaml instance of [[ApplicationYaml]]
   * @throws java.lang.ClassNotFoundException if JDBC driver class is not found
   * @throws java.sql.SQLException if connection's initialization fails
   * @return [[java.sql.Connection]]
   */

  @throws(classOf[ClassNotFoundException])
  @throws(classOf[SQLException])
  private def initImpalaJDBCConnection(yaml: ApplicationYaml): Connection = {

    val driverClassName: String = yaml.getProperty("impala.jdbc.driverClass")
    val impalaJdbcUrl: String = yaml.getProperty("impala.jdbc.url")

    Class.forName(driverClassName)
    val connection: Connection = DriverManager.getConnection(impalaJdbcUrl)
    log.info("Successfully initialized Impala JDBC connection with URL {}", impalaJdbcUrl)
    connection
  }
}
