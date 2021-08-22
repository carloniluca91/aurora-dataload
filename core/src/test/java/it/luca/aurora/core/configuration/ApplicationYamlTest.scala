package it.luca.aurora.core.configuration

import it.luca.aurora.core.configuration.yaml.{ApplicationYaml, DataSource}
import org.scalatest.flatspec.AnyFlatSpec

import java.util

class ApplicationYamlTest extends AnyFlatSpec {

  s"An instance of ${classOf[ApplicationYaml].getSimpleName}" must "properly interpolate properties" in {

    val (v0, v1) = ("v0", "v1")
    val (k2, v2) = ("k2", "${k0}")
    val (k3, v3) = ("k3", "${k0}/${k1}")
    val scalaMap: Map[String, String]=  Map("k0" -> v0, "k1" -> v1, k2 -> v2, k3 -> v3)
    val properties: util.Map[String, String] = new util.LinkedHashMap[String, String]
    scalaMap.foreach {
      case (key, value) => properties.put(key, value)
    }

    val applicationYaml = new ApplicationYaml(properties, new util.ArrayList[DataSource]())
    val interpolatedProperties = applicationYaml.withInterpolation().getProperties
    assertResult(applicationYaml.getProperties.size()) {
      interpolatedProperties.size()
    }

    // Assert all original keys are still present
    scalaMap.keys.foreach {
      key => assert(interpolatedProperties.containsKey(key))
    }

    // Assert single-variable interpolation
    assertResult(v0) {
      interpolatedProperties.get(k2)
    }

    // Assert double-variable interpolation
    assertResult(s"$v0/$v1") {
      interpolatedProperties.get(k3)
    }
  }
}