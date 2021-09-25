package it.luca.aurora.app.logging

import it.luca.aurora.configuration.datasource.DataSource
import it.luca.aurora.core.implicits._
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class DataloadJobLogRecord(applicationId: String,
                                applicationName: String,
                                applicationStartTime: Timestamp,
                                applicationStartDate: String,
                                dataSourceId: String,
                                metadataFilePath: String,
                                ingestedFile: String,
                                ingestionOperationCode: String,
                                exceptionClass: Option[String],
                                exceptionMessage: Option[String],
                                yarnApplicationUiUrl: String,
                                yarnApplicationLogCmd: String,
                                insertTs: Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                                insertDt: String = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE),
                                month: String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM")))

object DataloadJobLogRecord {

  val OK = "OK"
  val KO = "KO"

  /**
   * Create an instance of [[DataloadJobLogRecord]]
   *
   * @param sparkContext [[SparkContextWrapper]] of current Spark application
   * @param dataSource instance of [[DataSource]]
   * @param filePath [[Path]] of ingested file
   * @param yarnUiUrl Root Url of Yarn UI
   * @param exceptionOpt optional exception to be reported
   * @return instance of [[DataloadJobLogRecord]]
   */

  def apply(sparkContext: SparkContextWrapper,
            dataSource: DataSource,
            yarnUiUrl: String,
            filePath: Path,
            exceptionOpt: Option[Throwable]): DataloadJobLogRecord = {

    val appId: String = sparkContext.applicationId
    DataloadJobLogRecord(applicationId = appId,
      applicationName = sparkContext.appName,
      applicationStartTime = sparkContext.startTimeAsTimestamp,
      applicationStartDate = sparkContext.startTimeAsString("yyyy-MM-dd"),
      dataSourceId = dataSource.getId,
      metadataFilePath = dataSource.getMetadataFilePath,
      ingestedFile = filePath.toString,
      ingestionOperationCode = exceptionOpt.map(_ => KO).getOrElse(OK),
      exceptionClass = exceptionOpt.map(x => x.getClass.getName),
      exceptionMessage = exceptionOpt.map(x => x.getMessage),
      yarnApplicationUiUrl = s"$yarnUiUrl/$appId",
      yarnApplicationLogCmd = s"yarn logs -applicationId $appId >> ${appId}_${sparkContext.startTimeAsString("yyyy_MM_dd_HH_mm_ss")}.log")
  }
}
