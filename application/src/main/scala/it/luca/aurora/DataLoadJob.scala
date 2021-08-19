package it.luca.aurora

import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.utils.Utils.readProperties
import it.luca.aurora.option.CliArguments
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.spark.sql.SparkSession

import java.sql.{Connection, DriverManager, SQLException}

class DataLoadJob(protected val cliArguments: CliArguments)
  extends Logging {

  def run(): Unit = {

    val sparkSession: SparkSession = initSparkSession()
    val properties: PropertiesConfiguration = readProperties(cliArguments.propertiesFileName)
    log.info(s"Successfully loaded file ${cliArguments.propertiesFileName}")
    val impalaJDBCConnection: Connection = initImpalaJDBCConnection(properties)

  }

  /**
   * Initializes a [[java.sql.Connection]] to Impala via JDBC
   * @param properties Spark application properties
   * @throws java.lang.ClassNotFoundException if JDBC driver class is not found
   * @throws java.sql.SQLException if connection's initialization fails
   * @return [[java.sql.Connection]]
   */

  @throws(classOf[ClassNotFoundException])
  @throws(classOf[SQLException])
  private def initImpalaJDBCConnection(properties: PropertiesConfiguration): Connection = {

    val driverClassName = properties.getString("impala.jdbc.driverClass")
    val impalaJdbcUrl = properties.getString("impala.jdbc.url")

    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(impalaJdbcUrl)
    log.info("Successfully initialized Impala JDBC connection with URL {}", impalaJdbcUrl)
    connection
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
}
