package it.luca.aurora.app.job

import it.luca.aurora.configuration.datasource.DataSource
import it.luca.aurora.core.BasicMockingTest
import it.luca.aurora.core.implicits.SparkSessionWrapper
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DataloadJobLogRecordTest
  extends BasicMockingTest {

  private val (appId, appName) = ("appId", "appName")
  private val now = LocalDateTime.now()
  private val startTimeTs = Timestamp.valueOf(now)

  // Mock SparkContext
  private val scWrapper: SparkSessionWrapper = stub[SparkSessionWrapper]
  (scWrapper.applicationId _).when().returns(appId)
  (scWrapper.appName _).when().returns(appName)
  (scWrapper.startTimeAsTimestamp _).when().returns(startTimeTs)
  (scWrapper.startTimeAsString _).when("yyyy-MM-dd").returns(now.format(DateTimeFormatter.ISO_LOCAL_DATE))

  private val dataSource = DataSource("dataSourceId", "metadataFilePath")
  private val filePath: Path = new Path("/file/status/path")
  private val yarnUiUrl = "yarnUiUrl"

  s"A ${nameOf[DataloadJobLogRecord]}" should s"be correctly initialized" in {

    val exceptionOpt: Option[Throwable] = None
    val record = DataloadJobLogRecord(scWrapper, dataSource, yarnUiUrl, filePath, exceptionOpt)
    record.applicationId shouldEqual appId
    record.applicationName shouldEqual appName
    record.applicationStartTime shouldEqual startTimeTs
    record.applicationStartDate shouldEqual startTimeTs
      .toLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE)

    record.dataSourceId shouldEqual dataSource.id
    record.metadataFilePath shouldEqual dataSource.metadataFilePath
    record.ingestedFile shouldEqual filePath.toString
  }

  it should s"report exception class and message if a non-empty ${nameOf[Throwable]} is provided" in {

    val throwable: IllegalArgumentException = new IllegalArgumentException("exceptionMsg")
    val record = DataloadJobLogRecord(scWrapper, dataSource, yarnUiUrl, filePath, Some(throwable))

    record.ingestionOperationCode shouldEqual DataloadJobLogRecord.KO
    record.exceptionClass shouldEqual Some(throwable.getClass.getName)
    record.exceptionMessage shouldEqual Some(throwable.getMessage)
  }

  it should s"not report anything if an empty ${nameOf[Throwable]} is provided" in {

    val record = DataloadJobLogRecord(scWrapper, dataSource, yarnUiUrl, filePath, None)

    record.ingestionOperationCode shouldEqual DataloadJobLogRecord.OK
    record.exceptionClass shouldEqual None
    record.exceptionMessage shouldEqual None
  }
}
