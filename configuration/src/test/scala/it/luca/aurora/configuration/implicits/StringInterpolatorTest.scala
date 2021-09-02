package it.luca.aurora.configuration.implicits

import it.luca.aurora.configuration.yaml.{ApplicationYaml, DataSource}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util
import scala.collection.JavaConversions._

class StringInterpolatorTest
  extends AnyFlatSpec
    with should.Matchers {

  private val (k1, v1) = ("k1", "v1")
  private val (k2, v2) = ("second.property", "v2")
  private val properties: util.Map[String, String] = new util.HashMap[String, String]() {{
    put(k1, v1)
    put(k2, v2)
  }}

  private val yaml: ApplicationYaml = new ApplicationYaml(properties, new util.ArrayList[DataSource]())

  s"a ${classOf[StringInterpolator].getSimpleName}" should "correctly interpolate a string" in {

    val sep = ", "
    val string: String = properties.zipWithIndex.map {
      case ((key, _), i) =>
        s"value$i: ".concat("${%s}".format(key))
    }.mkString(sep)

    val expectedString: String = properties.zipWithIndex.map {
      case ((_, value), i) =>
      s"value$i: $value"
    }.mkString(sep)

    string.interpolateUsingYaml(yaml) shouldEqual expectedString
  }
}
