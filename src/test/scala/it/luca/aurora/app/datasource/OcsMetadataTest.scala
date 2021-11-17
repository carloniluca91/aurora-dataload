package it.luca.aurora.app.datasource

import it.luca.aurora.configuration.metadata.extract.{CsvExtract, Extract}
import it.luca.aurora.configuration.metadata.transform.{FileNamePartitioning, Partitioning, Transform}

class OcsMetadataTest
  extends DataSourceMetadataTest("OCS") {

  override protected def testExtract(extract: Extract): Unit = {

    extract.isInstanceOf[CsvExtract] shouldBe true
    val csvExtract = extract.asInstanceOf[CsvExtract]
    csvExtract.options.isDefined shouldBe true
  }

  override protected def testTransform(transform: Transform): Unit = {

    val dropDuplicatesOpt: Option[Seq[String]] = transform.dropDuplicates
    dropDuplicatesOpt shouldBe Some(_: Seq[String])
    dropDuplicatesOpt.get.size shouldEqual 2
    transform.dropColumns shouldBe None
  }

  override protected def testPartitioning(partitioning: Partitioning): Unit = {

    partitioning.isInstanceOf[FileNamePartitioning] shouldBe true
  }
}
