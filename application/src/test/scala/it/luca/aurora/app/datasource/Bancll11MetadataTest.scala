package it.luca.aurora.app.datasource
import it.luca.aurora.configuration.metadata.extract.{CsvExtract, Extract}
import it.luca.aurora.configuration.metadata.load.{FileNameRegexInfo, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform

class Bancll11MetadataTest
  extends DataSourceMetadataTest("BANCLL11") {

  override protected def testExtract(extract: Extract): Unit = {

    extract.isInstanceOf[CsvExtract] shouldBe true
    extract.asInstanceOf[CsvExtract].getConfiguration.getOptions.isEmpty shouldBe false
  }

  override protected def testTransform(transform: Transform): Unit = {

    Option(transform.getDropDuplicates) shouldBe None
    Option(transform.getDropColumns) shouldBe None
  }

  override protected def testPartitionInfo(partitionInfo: PartitionInfo): Unit = {

    partitionInfo.isInstanceOf[FileNameRegexInfo] shouldBe true
  }
}
