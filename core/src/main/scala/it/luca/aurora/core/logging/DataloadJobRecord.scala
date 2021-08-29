package it.luca.aurora.core.logging

import it.luca.aurora.core.configuration.yaml.DataSource
import it.luca.aurora.core.implicits._
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class DataloadJobRecord(applicationId: String,
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
                             insertTs: Timestamp,
                             insertDt: String,
                             month: String)

object DataloadJobRecord {

  /**
   * Create an instance of [[DataloadJobRecord]]
   * @param scWrapper implicit [[SparkContextWrapper]] of current Spark application
   * @param dataSource instance of [[DataSource]]
   * @param filePath [[Path]] of ingested file
   * @param yarnUiUrl Root Url of Yarn UI
   * @param exceptionOpt optional exception to be reported
   * @return instance of [[DataloadJobRecord]]
   */

  def apply(scWrapper: SparkContextWrapper,
            dataSource: DataSource,
            filePath: Path,
            yarnUiUrl: String,
            exceptionOpt: Option[Throwable]): DataloadJobRecord = {

    val now = LocalDateTime.now()
    val appId: String = scWrapper.applicationId

    DataloadJobRecord(applicationId = appId,
      applicationName = scWrapper.appName,
      applicationStartTime = scWrapper.startTimeAsTimestamp,
      applicationStartDate = scWrapper.startTimeAsString("yyyy-MM-dd"),
      dataSourceId = dataSource.getId,
      metadataFilePath = dataSource.getMetadataFilePath,
      ingestedFile = filePath.toString,
      ingestionOperationCode = exceptionOpt.map(_ => "KO").getOrElse("OK"),
      exceptionClass = exceptionOpt.map(x => x.getClass.getName),
      exceptionMessage = exceptionOpt.map(x => x.getMessage),
      yarnApplicationUiUrl = s"$yarnUiUrl/$appId",
      yarnApplicationLogCmd = s"yarn logs -applicationId $appId >> ${appId}_${scWrapper.startTimeAsString("yyyy_MM_dd_HH_mm_ss")}.log",
      insertTs = Timestamp.valueOf(now),
      insertDt = now.format(DateTimeFormatter.ISO_LOCAL_DATE),
      month = now.format(DateTimeFormatter.ofPattern("yyyy-MM")))
  }
}
