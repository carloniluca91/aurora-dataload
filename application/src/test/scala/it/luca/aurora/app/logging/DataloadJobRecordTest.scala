package it.luca.aurora.app.logging

import it.luca.aurora.configuration.yaml.DataSource
import it.luca.aurora.core.implicits.SparkContextWrapper
import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DataloadJobRecordTest
  extends AnyFlatSpec
    with should.Matchers
    with MockFactory {

  private val (appId, appName) = ("appId", "appName")
  private val now = LocalDateTime.now()
  private val startTimeTs = Timestamp.valueOf(now)

  // Mock SparkContext
  private val scWrapper: SparkContextWrapper = stub[SparkContextWrapper]
  (scWrapper.applicationId _).when().returns(appId)
  (scWrapper.appName _).when().returns(appName)
  (scWrapper.startTimeAsTimestamp _).when().returns(startTimeTs)
  (scWrapper.startTimeAsString _).when("yyyy-MM-dd").returns(now.format(DateTimeFormatter.ISO_LOCAL_DATE))

  private val dataSource = new DataSource("dataSourceId", "metadataFilePath")
  private val filePath: Path = new Path("/file/status/path")
  private val yarnUiUrl = "yarnUiUrl"

  s"A ${classOf[DataloadJobRecord].getSimpleName}" should
    s"be correctly initialized" in {

    val exceptionOpt: Option[Throwable] = None
    val record = DataloadJobRecord(scWrapper, dataSource, yarnUiUrl, filePath, exceptionOpt)
    record.applicationId shouldEqual appId
    record.applicationName shouldEqual appName
    record.applicationStartTime shouldEqual startTimeTs
    record.applicationStartDate shouldEqual startTimeTs
      .toLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE)

    record.dataSourceId shouldEqual dataSource.getId
    record.metadataFilePath shouldEqual dataSource.getMetadataFilePath
    record.ingestedFile shouldEqual filePath.toString
  }

  it should s"report exception class and message if a non-empty ${classOf[Option[Throwable]].getSimpleName} is provided" in {

    val throwable: IllegalArgumentException = new IllegalArgumentException("exceptionMsg")
    val record = DataloadJobRecord(scWrapper, dataSource, yarnUiUrl, filePath, Some(throwable))

    record.ingestionOperationCode shouldEqual "KO"
    record.exceptionClass shouldEqual Some(throwable.getClass.getName)
    record.exceptionMessage shouldEqual Some(throwable.getMessage)
  }

  it should s"not report anything if an empty ${classOf[Option[Throwable]].getSimpleName} is provided" in {

    val record = DataloadJobRecord(scWrapper, dataSource, yarnUiUrl, filePath, None)

    record.ingestionOperationCode shouldEqual "OK"
    record.exceptionClass shouldEqual None
    record.exceptionMessage shouldEqual None
  }
}
