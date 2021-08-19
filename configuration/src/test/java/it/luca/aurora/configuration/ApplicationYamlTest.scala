package it.luca.aurora.configuration

import org.scalatest.flatspec.AnyFlatSpec

import java.util

class ApplicationYamlTest extends AnyFlatSpec {

  s"A ${classOf[ApplicationYaml].getSimpleName}" should "interpolate properties" in {

    val FirstProperty = "first.property"
    val SecondProperty = "second.property"
    val properties: util.Map[String, String] = new util.HashMap[String, String]{{
      put(FirstProperty, "v1")
      put(SecondProperty, "${first.property}/v2")
    }}

    val applicationYaml = new ApplicationYaml(properties, new util.ArrayList[DataSource]())
    val interpolatedProperties = applicationYaml.withInterpolation().getProperties
    assertResult(applicationYaml.getProperties.size()) {
      interpolatedProperties.size()
    }

    assert(interpolatedProperties.containsKey(FirstProperty))
    assert(interpolatedProperties.containsKey(SecondProperty))
    assertResult("v1/v2") {
      interpolatedProperties.get(SecondProperty)
    }
  }
}
