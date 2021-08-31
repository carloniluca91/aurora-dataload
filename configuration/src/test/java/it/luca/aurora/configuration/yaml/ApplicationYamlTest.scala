package it.luca.aurora.configuration.yaml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util

class ApplicationYamlTest
  extends AnyFlatSpec
  with should.Matchers {

  private val (v0, v1) = ("v0", "v1")
  private val (k2, v2) = ("k2", "${k0}")
  private val (k3, v3) = ("k3", "${k0}/${k1}")
  private val scalaMap: Map[String, String]=  Map("k0" -> v0, "k1" -> v1, k2 -> v2, k3 -> v3)
  private val properties: util.Map[String, String] = new util.LinkedHashMap[String, String]
  scalaMap.foreach {
    case (key, value) => properties.put(key, value)
  }

  s"An instance of ${classOf[ApplicationYaml].getSimpleName}" should
    "properly interpolate properties" in {

    val applicationYaml = new ApplicationYaml(properties, new util.ArrayList[DataSource]())
    val interpolatedProperties = applicationYaml.withInterpolation().getProperties
    applicationYaml.getProperties.size() shouldEqual interpolatedProperties.size()

    // Assert all original keys are still present
    scalaMap.keys.foreach {
      key => assert(interpolatedProperties.containsKey(key))
    }

    // Assert single-variable interpolation
    interpolatedProperties.get(k2) shouldEqual v0

    // Assert double-variable interpolation
    interpolatedProperties.get(k3) shouldEqual s"$v0/$v1"
  }

  it should
    s"throw an ${classOf[UnExistingPropertyException].getSimpleName} if an unexisting key is provided" in {

    val unexistingProperty = "unexistingProperty"
    properties.containsKey(unexistingProperty) shouldBe false
    val applicationYaml = new ApplicationYaml(properties, new util.ArrayList[DataSource]())
    an [UnExistingPropertyException] should be thrownBy {
      applicationYaml.getProperty(unexistingProperty)
    }
  }

  it should
    s"throw an ${classOf[UnExistingDataSourceException].getSimpleName} if an unexisting dataSourceId is provided" in {

    val dataSources: util.List[DataSource] = util.Arrays.asList(new DataSource("dsId", "path"))
    val applicationYaml = new ApplicationYaml(properties, dataSources)
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

    val applicationYaml = new ApplicationYaml(properties, dataSources)
    a [DuplicatedDataSourceException] should be thrownBy {
      applicationYaml.getDataSourceWithId(dataSourceId)
    }
  }
}