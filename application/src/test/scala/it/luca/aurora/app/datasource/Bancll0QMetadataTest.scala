package it.luca.aurora.app.datasource
import it.luca.aurora.configuration.metadata.extract.{CsvExtract, Extract}
import it.luca.aurora.configuration.metadata.load.{FileNameRegexInfo, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform

class Bancll0QMetadataTest
  extends DataSourceMetadataTest("BANCLL0Q") {

  override protected def testExtract(extract: Extract): Unit = {

    extract.isInstanceOf[CsvExtract] shouldBe true
    extract.asInstanceOf[CsvExtract].getConfiguration.getOptions.isEmpty shouldBe false
  }

  override protected def testTransform(transform: Transform): Unit = {

    Option(transform.getDropColumns) shouldBe None
    Option(transform.getDropDuplicates) shouldBe None
  }

  override protected def testPartitionInfo(partitionInfo: PartitionInfo): Unit = {

    partitionInfo.isInstanceOf[FileNameRegexInfo] shouldBe true
  }
}
