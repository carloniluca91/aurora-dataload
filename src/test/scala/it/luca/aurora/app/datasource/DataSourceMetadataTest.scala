package it.luca.aurora.app.datasource

import it.luca.aurora.configuration.DataSourceWrapper
import it.luca.aurora.configuration.JsonDeserializer.{deserializeResourceAs, deserializeStringAs}
import it.luca.aurora.configuration.datasource.DataSource
import it.luca.aurora.configuration.metadata.DataSourceMetadata
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.transform.{Partitioning, Transform}
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import it.luca.aurora.core.utils.{loadPropertiesResource, replaceTokensWithProperties}
import it.luca.aurora.core.{BasicTest, Logging}
import org.apache.commons.configuration2.PropertiesConfiguration

import java.io.InputStream
import scala.io.Source
import scala.util.{Success, Try}

abstract class DataSourceMetadataTest(protected val dataSourceId: String)
  extends BasicTest
    with Logging {

  protected def mkString[T](seq: Seq[T]): String = "\n\n"
    .concat(seq.map { x => s"  $x"}.mkString("\n"))
    .concat("\n")

  protected def shouldBeASuccess[T, R](instance: T, function: T => R): Unit = Try { function(instance) } shouldBe Success(_: R)

  s"Metadata file for dataSource $dataSourceId" should
    s"be correctly deserialized as a ${classOf[DataSourceMetadata].getSimpleName} instance" in {

    // Read .properties and dataSources .json
    val properties: PropertiesConfiguration = loadPropertiesResource("spark_application.properties")
    val toStream: String => InputStream = s => this.getClass.getClassLoader.getResourceAsStream(s)
    val dataSourcesWrapper: DataSourceWrapper = deserializeResourceAs("aurora_datasources.json", classOf[DataSourceWrapper])
    val dataSource: DataSource = dataSourcesWrapper.getDataSourceWithId(dataSourceId)

    // Interpolate metadata .json string with application .properties and deserialize as Java object
    val metadataJsonString: String = replaceTokensWithProperties(Source
      .fromInputStream(toStream(dataSource.metadataFilePath))
      .getLines().mkString("\n"), properties)

    // Test configured dataSource
    val dataSourceMetadata: DataSourceMetadata = deserializeStringAs(metadataJsonString, classOf[DataSourceMetadata])
    val extract: Extract = dataSourceMetadata.extract
    testExtract(extract)

    // Transform
    val transform: Transform = dataSourceMetadata.transform
    transform.filters.isEmpty shouldBe false
    transform.transformations.isEmpty shouldBe false

    val failingFilters: Seq[String] = transform.filters.filter { s => Try { SqlExpressionParser.parse(s) }.isFailure }
    if (failingFilters.nonEmpty) log.warn(s"Failing filters: ${mkString(failingFilters)}")
    failingFilters shouldBe empty

    // Transformations
    val failingTransformations: Seq[String] = transform.transformations.filter { s => Try { SqlExpressionParser.parse(s) }.isFailure}
    if (failingTransformations.nonEmpty) log.warn(s"Failing transformations: ${mkString(failingTransformations)}")
    failingTransformations shouldBe empty
    testTransform(transform)

    // Partitioning
    testPartitioning(transform.partitioning)
  }

  protected def testExtract(extract: Extract): Unit

  protected def testTransform(transform: Transform): Unit

  protected def testPartitioning(partitioning: Partitioning): Unit
}
