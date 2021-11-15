package it.luca.aurora.app.datasource

import it.luca.aurora.configuration.metadata.extract.{CsvExtract, Extract}
import it.luca.aurora.configuration.metadata.transform.{FileNamePartitioning, Partitioning, Transform}

class Bancll0QMetadataTest
  extends DataSourceMetadataTest("BANCLL0Q") {

  override protected def testExtract(extract: Extract): Unit = {

    extract.isInstanceOf[CsvExtract] shouldBe true
    extract.asInstanceOf[CsvExtract].options shouldBe Some(_: Map[String, String])
  }

  override protected def testTransform(transform: Transform): Unit = {

    transform.dropColumns shouldBe None
    transform.dropDuplicates shouldBe None
  }

  override protected def testPartitioning(partitioning: Partitioning): Unit = {

    partitioning.isInstanceOf[FileNamePartitioning] shouldBe true
  }
}
