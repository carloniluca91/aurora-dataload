package it.luca.aurora.app.datasource

import it.luca.aurora.app.utils.{loadProperties, replaceTokensWithProperties}
import it.luca.aurora.configuration.ObjectDeserializer.{deserializeStream, deserializeString}
import it.luca.aurora.configuration.datasource.{DataSource, DataSourcesWrapper}
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.load.{Load, PartitionInfo}
import it.luca.aurora.configuration.metadata.transform.Transform
import it.luca.aurora.core.Logging
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.commons.configuration2.PropertiesConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.io.InputStream
import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Try

abstract class DataSourceMetadataTest(protected val dataSourceId: String)
  extends AnyFlatSpec
    with should.Matchers
    with Logging {

  protected def mkString[T](seq: Seq[T]): String = seq.map { x => s"  $x"}.mkString("\n").concat("\n")

  s"Metadata file for dataSource $dataSourceId" should
    s"be correctly deserialized as a ${classOf[DataSourceMetadata].getSimpleName} instance" in {

    // Read .properties
    val properties: PropertiesConfiguration = loadProperties("spark_application.properties")

    // Read .yaml
    val toStream: String => InputStream = s => this.getClass.getClassLoader.getResourceAsStream(s)
    val applicationYaml = deserializeStream(toStream("aurora_datasources.json"), classOf[DataSourcesWrapper])

    val dataSource: DataSource = applicationYaml.getDataSourceWithId(dataSourceId)
      .withInterpolation(properties)

    // Interpolate metadata .json string with application .properties and deserialize as Java object
    val metadataJsonString: String = replaceTokensWithProperties(Source
      .fromInputStream(toStream(dataSource.getMetadataFilePath))
      .getLines().mkString("\n"), properties)

    val dataSourceMetadata: DataSourceMetadata = deserializeString(metadataJsonString, classOf[DataSourceMetadata])

    // Test extract
    val extract: Extract = dataSourceMetadata.getEtlConfiguration.getExtract
    testExtract(extract)

    // Test transform
    val transform: Transform = dataSourceMetadata.getEtlConfiguration.getTransform
    transform.getFilters.isEmpty shouldBe false
    val failingFilters: Seq[String] = transform.getFilters.filter {
      s => Try { SqlExpressionParser.parse(s) }.isFailure
    }

    // Filters
    if (failingFilters.nonEmpty) log.warn(s"Failing filters: ${mkString(failingFilters)}")
    failingFilters.isEmpty shouldBe true

    // Transformations
    val failingTransformations: Seq[String] = transform.getTransformations.filter {
      s => Try { SqlExpressionParser.parse(s) }.isFailure
    }

    if (failingTransformations.nonEmpty) log.warn(s"Failing transformations: ${mkString(failingTransformations)}")
    failingTransformations.isEmpty shouldBe true
    testTransform(transform)

    // Test load
    val load: Load = dataSourceMetadata.getEtlConfiguration.getLoad
    testPartitionInfo(load.getPartitionInfo)
  }

  protected def testExtract(extract: Extract): Unit

  protected def testTransform(transform: Transform): Unit

  protected def testPartitionInfo(partitionInfo: PartitionInfo): Unit
}
