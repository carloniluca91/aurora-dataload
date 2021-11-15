package it.luca.aurora.app.datasource

import it.luca.aurora.configuration.metadata.extract.{AvroExtract, Extract}
import it.luca.aurora.configuration.metadata.transform.{FileNamePartitioning, Partitioning, Transform}

class AmexMetadataTest
  extends DataSourceMetadataTest("AMEX") {

  override protected def testExtract(extract: Extract): Unit = {

    extract.isInstanceOf[AvroExtract] shouldBe true
  }

  override protected def testTransform(transform: Transform): Unit = {

    transform.dropColumns shouldBe None
    transform.dropDuplicates shouldBe None
  }

  override protected def testPartitioning(partitioning: Partitioning): Unit = {

    partitioning.isInstanceOf[FileNamePartitioning] shouldBe true
  }
}
