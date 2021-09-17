package it.luca.aurora.app.datasource

import it.luca.aurora.configuration.metadata.extract.{CsvExtract, Extract}
import it.luca.aurora.configuration.metadata.load.{FileNameRegexInfo, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform

import scala.collection.JavaConversions._

class OcsMetadataTest
  extends DataSourceMetadataTest("ocs.json") {

  override protected def testExtract(extract: Extract): Unit = {

    extract.isInstanceOf[CsvExtract] shouldBe true
    val csvExtract = extract.asInstanceOf[CsvExtract]
    csvExtract.getConfiguration.getOptions.isEmpty shouldBe false
  }

  override protected def testTransform(transform: Transform): Unit = {

    val dropDuplicatesCols: Seq[String] = transform.getDropDuplicates
    dropDuplicatesCols.isEmpty shouldBe false
    dropDuplicatesCols.size shouldEqual 2
  }

  override protected def testPartitionInfo(partitionInfo: PartitionInfo): Unit = {

    partitionInfo.isInstanceOf[FileNameRegexInfo] shouldBe true
  }
}
