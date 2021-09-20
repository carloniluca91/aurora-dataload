package it.luca.aurora.configuration.yaml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util

class ApplicationYamlTest
  extends AnyFlatSpec
  with should.Matchers {

  s"An ${classOf[ApplicationYaml].getSimpleName}" should
    s"throw an ${classOf[UnExistingDataSourceException].getSimpleName} if an unexisting dataSourceId is provided" in {

    val dataSources: util.List[DataSource] = util.Arrays.asList(new DataSource("dsId", "path"))
    val applicationYaml = new ApplicationYaml(dataSources)
    an [UnExistingDataSourceException] should be thrownBy {
      applicationYaml.getDataSourceWithId("unexistingId")
    }
  }

  it should
    s"throw a ${classOf[DuplicatedDataSourceException].getSimpleName} if case of more ${classOf[DataSource].getSimpleName}(s) sharing the same id" in {

    val dataSourceId = "dsId"
    val dataSources: util.List[DataSource] = util.Arrays.asList(
      new DataSource(dataSourceId, "firstPath"),
      new DataSource(dataSourceId, "secondPath"))

    val applicationYaml = new ApplicationYaml(dataSources)
    a [DuplicatedDataSourceException] should be thrownBy {
      applicationYaml.getDataSourceWithId(dataSourceId)
    }
  }
}