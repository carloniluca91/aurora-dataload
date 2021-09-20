package it.luca.aurora.configuration.implicits

import org.apache.commons.configuration2.PropertiesConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class StringInterpolatorTest
  extends AnyFlatSpec
    with should.Matchers {

  private val (k1, v1) = ("k1", "v1")
  private val (k2, v2) = ("second.property", "v2")
  private val map: Map[String, String] = Map(k1 -> v1,
    k2 -> v2)

  s"a ${classOf[StringInterpolator].getSimpleName}" should
    s"correctly interpolate a string using an instance of ${classOf[PropertiesConfiguration].getSimpleName}" in {

    val properties: PropertiesConfiguration = new PropertiesConfiguration
    map.foreach {
      case (k, v) => properties.setProperty(k, v)
    }

    val sep = ", "
    val string: String = map.zipWithIndex.map {
      case ((key, _), i) =>
        s"value$i: ".concat("${%s}".format(key))
    }.mkString(sep)

    val expectedString: String = map.zipWithIndex.map {
      case ((_, value), i) =>
      s"value$i: $value"
    }.mkString(sep)

    string.withInterpolation(properties) shouldEqual expectedString
  }
}
