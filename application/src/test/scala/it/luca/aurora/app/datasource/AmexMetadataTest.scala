package it.luca.aurora.app.datasource
import it.luca.aurora.configuration.metadata.extract.{AvroExtract, Extract}
import it.luca.aurora.configuration.metadata.load.{FileNameRegexInfo, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform

class AmexMetadataTest
  extends DataSourceMetadataTest("AMEX") {

  override protected def testExtract(extract: Extract): Unit = {

    extract.isInstanceOf[AvroExtract] shouldBe true
  }

  override protected def testTransform(transform: Transform): Unit = {

    Option(transform.getDropColumns) shouldBe None
    Option(transform.getDropDuplicates) shouldBe None
  }

  override protected def testPartitionInfo(partitionInfo: PartitionInfo): Unit = {

    partitionInfo.isInstanceOf[FileNameRegexInfo] shouldBe true
  }
}
