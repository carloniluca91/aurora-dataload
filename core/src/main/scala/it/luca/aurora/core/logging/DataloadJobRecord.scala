package it.luca.aurora.core.logging

import it.luca.aurora.core.configuration.yaml.DataSource
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.SparkContext

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
   * Create an instance of [[it.luca.aurora.core.logging.DataloadJobRecord]]
   *
   * @param sparkContext instance of [[org.apache.spark.SparkContext]]
   * @param dataSource instance of [[it.luca.aurora.core.configuration.yaml.DataSource]]
   * @param yarnUiUrl Root Url of Yarn UI
   * @param exceptionOpt optional exception to be logged
   * @return instance of [[it.luca.aurora.core.logging.DataloadJobRecord]]
   */

  def apply(sparkContext: SparkContext,
            dataSource: DataSource,
            fileStatus: FileStatus,
            yarnUiUrl: String,
            exceptionOpt: Option[Throwable]): DataloadJobRecord = {

    val now = LocalDateTime.now()
    val appId: String = sparkContext.applicationId
    val appStartTime = new Timestamp(sparkContext.startTime)
    val appStartTimeLdtm: LocalDateTime = appStartTime.toLocalDateTime
    val appStartTimeFormatted: String = appStartTimeLdtm
      .format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss"))

    DataloadJobRecord(applicationId = appId,
      applicationName = sparkContext.appName,
      applicationStartTime = appStartTime,
      applicationStartDate = appStartTimeLdtm.format(DateTimeFormatter.ISO_LOCAL_DATE),
      dataSourceId = dataSource.getId,
      metadataFilePath = dataSource.getMetadataFilePath,
      ingestedFile = fileStatus.getPath.toString,
      ingestionOperationCode = exceptionOpt.map(_ => "KO").getOrElse("OK"),
      exceptionClass = exceptionOpt.map(x => x.getClass.getName),
      exceptionMessage = exceptionOpt.map(x => x.getMessage),
      yarnApplicationUiUrl = s"$yarnUiUrl/$appId",
      yarnApplicationLogCmd = s"yarn logs -applicationId $appId >> ${appId}_$appStartTimeFormatted.log",
      insertTs = Timestamp.valueOf(now),
      insertDt = now.format(DateTimeFormatter.ISO_LOCAL_DATE),
      month = now.format(DateTimeFormatter.ofPattern("yyyy-MM")))
  }
}
