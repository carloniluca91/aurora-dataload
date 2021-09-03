package it.luca.aurora.configuration.implicits

import it.luca.aurora.configuration.yaml.{ApplicationYaml, UnExistingPropertyException}

import scala.util.matching.Regex

class StringInterpolator(protected val string: String) {

  /**
   * Interpolates this string using as instance of [[ApplicationYaml]]
   * @param yaml instance of [[ApplicationYaml]]
   * @throws it.luca.aurora.configuration.yaml.UnExistingPropertyException if the value of an unknown property is requested
   * @return interpolated string (e.g. a token like ${a.property} is replaced with the value of property
   *         'a.property' retrieved from the instance of [[ApplicationYaml]] (if present))
   */

  @throws[UnExistingPropertyException]
  def interpolateUsingYaml(yaml: ApplicationYaml): String = {

    val regex: Regex = "\\$\\{([\\w.]+)}".r
    regex.replaceAllIn(string, m => s"${yaml.getProperty(m.group(1))}")
  }
}
