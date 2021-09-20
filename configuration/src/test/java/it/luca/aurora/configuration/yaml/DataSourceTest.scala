package it.luca.aurora.configuration.yaml

import org.apache.commons.configuration2.PropertiesConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class DataSourceTest
  extends AnyFlatSpec
    with should.Matchers {

  s"A ${classOf[DataSource]}" should "" in {

    val (key, value, fileName) = ("datasources.metadata.root", "/user/apps/metadata", "file.json")
    val properties = new PropertiesConfiguration
    properties.setProperty(key, value)
    val metadataFilePath = String.format("${%s}/%s", key,fileName)
    val dataSource = new DataSource("ID", metadataFilePath)
      .withInterpolation(properties)

    dataSource.getMetadataFilePath shouldEqual s"$value/$fileName"
  }
}
