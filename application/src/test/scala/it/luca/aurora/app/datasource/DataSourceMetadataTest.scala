package it.luca.aurora.app.datasource

import it.luca.aurora.configuration.implicits._
import it.luca.aurora.configuration.ObjectDeserializer.{DataFormat, deserializeStream, deserializeString}
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.load.{Load, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform
import it.luca.aurora.configuration.yaml.ApplicationYaml
import it.luca.aurora.core.Logging
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.io.InputStream
import scala.io.Source
import scala.collection.JavaConversions._
import scala.util.Try

abstract class DataSourceMetadataTest(protected val fileName: String)
  extends AnyFlatSpec
    with should.Matchers
    with Logging {

  protected final def isPresent[T](input: T): Boolean = Option(input).isDefined

  s"Metadata file $fileName" should
    s"be correctly deserialized as a ${classOf[DataSourceMetadata].getSimpleName} instance" in {

    // Read application yaml
    val toStream: String => InputStream = s => this.getClass.getClassLoader.getResourceAsStream(s)
    val applicationYaml = deserializeStream(toStream("spark_application.yaml"),
      classOf[ApplicationYaml],
      DataFormat.YAML).withInterpolation()

    // Interpolate metadata .json string with application yaml and deserialize as Java object
    val metadataJsonString: String = Source.fromInputStream(toStream(fileName))
      .getLines().mkString("\n")
      .interpolateUsingYaml(applicationYaml)
    val dataSourceMetadata = deserializeString(metadataJsonString, classOf[DataSourceMetadata], DataFormat.JSON)
    isPresent(dataSourceMetadata.getId) shouldBe true
    isPresent(dataSourceMetadata.getDataSourcePaths) shouldBe true
    isPresent(dataSourceMetadata.getEtlConfiguration) shouldBe true

    // Test extract
    val extract: Extract = dataSourceMetadata.getEtlConfiguration.getExtract
    isPresent(extract) shouldBe true
    testExtract(extract)

    // Test transform
    val transform: Transform = dataSourceMetadata.getEtlConfiguration.getTransform
    isPresent(transform) shouldBe true
    transform.getFilters.isEmpty shouldBe false
    val failingFilters: Seq[String] = transform.getFilters.filter {
      s => Try { SqlExpressionParser.parse(s) }.isFailure
    }

    // Filters
    if (failingFilters.nonEmpty) {
      log.warn(s"Failing filters: ${failingFilters.mkString("|")}")
    }
    failingFilters.isEmpty shouldBe true

    // Transformations
    val failingTransformations: Seq[String] = transform.getTransformations
      .filter { s =>
        Try { SqlExpressionParser.parse(s) }.isFailure
      }

    if (failingTransformations.nonEmpty) {
      log.warn(s"Failing transformations: ${failingTransformations.mkString("|")}")
    }
    failingTransformations.isEmpty shouldBe true

    testTransform(transform)

    // Test load
    val load: Load = dataSourceMetadata.getEtlConfiguration.getLoad
    isPresent(load) shouldBe true
    isPresent(load.getTarget) shouldBe true
    isPresent(load.getPartitionInfo) shouldBe true
    testPartitionInfo(load.getPartitionInfo)
  }

  protected def testExtract(extract: Extract): Unit

  protected def testTransform(transform: Transform): Unit

  protected def testPartitionInfo(partitionInfo: PartitionInfo): Unit
}
