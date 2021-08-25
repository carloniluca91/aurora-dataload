package it.luca.aurora.core.logging

import it.luca.aurora.core.configuration.yaml.DataSource
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Timestamp
import java.time.format.DateTimeFormatter

class DataloadJobRecordTest
  extends AnyFlatSpec
    with should.Matchers
    with MockFactory {

  /*
  private val (appId, appName, startTime) = ("appId", "appName", System.currentTimeMillis())

  // Mock SparkContext
  private val sparkContext: SparkContext = stub[SparkContext]
  (sparkContext.applicationId _).when().returns(appId)
  (sparkContext.appName _).when().returns(appName)
  (sparkContext.startTime _).when().returns(startTime)

  private val dataSource = new DataSource("dataSourceId", "metadataFilePath")

  // Mock FileStatus
  private val fileStatusPath: Path = stub[Path]
  private val fileStatus: FileStatus = stub[FileStatus]
  (fileStatusPath.toString _).when().returns("/file/status/path")
  (fileStatus.getPath _).when().returns(fileStatusPath)

  private val yarnUiUrl = "yarnUiUrl"

  s"A ${classOf[DataloadJobRecord].getSimpleName}" should
    s"be correctly initialized" in {

    val exceptionOpt: Option[Throwable] = None
    val record = DataloadJobRecord(sparkContext, dataSource, fileStatus, yarnUiUrl, exceptionOpt)
    record.applicationId shouldEqual appId
    record.applicationName shouldEqual appName
    record.applicationStartTime shouldEqual new Timestamp(startTime)
    record.applicationStartDate shouldEqual new Timestamp(startTime)
      .toLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE)

    record.dataSourceId shouldEqual dataSource.getId
    record.metadataFilePath shouldEqual dataSource.getMetadataFilePath
    record.ingestedFile shouldEqual fileStatusPath.toString
    }

  it should s"report exception class and message if a non-empty ${classOf[Option[Throwable]].getSimpleName} is provided" in {

    val throwable: IllegalArgumentException = new IllegalArgumentException("exceptionMsg")
    val record = DataloadJobRecord(sparkContext, dataSource, fileStatus, yarnUiUrl, Some(throwable))

    record.ingestionOperationCode shouldEqual "KO"
    record.exceptionClass shouldEqual Some(throwable.getClass.getName)
    record.exceptionMessage shouldEqual Some(throwable.getMessage)
  }

  it should s"report anything if an empty ${classOf[Option[Throwable]].getSimpleName} is provided" in {

    val record = DataloadJobRecord(sparkContext, dataSource, fileStatus, yarnUiUrl, None)

    record.ingestionOperationCode shouldEqual "OK"
    record.exceptionClass shouldEqual None
    record.exceptionMessage shouldEqual None
  }

   */
}
