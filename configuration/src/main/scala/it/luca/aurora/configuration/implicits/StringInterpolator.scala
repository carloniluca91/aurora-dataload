package it.luca.aurora.configuration.implicits

import it.luca.aurora.configuration.implicits.StringInterpolator.TokenReplaceRegex
import org.apache.commons.configuration2.PropertiesConfiguration

import scala.util.matching.Regex

class StringInterpolator(protected val string: String) {

  /**
   * Interpolates this string using as instance of [[PropertiesConfiguration]]
   * @param properties instance of [[PropertiesConfiguration]]
   * @return interpolated string (e.g. a token like ${a.property} is replaced with the value of property
   *         'a.property' retrieved from given instance of [[PropertiesConfiguration]]
   */

  def withInterpolation(properties: PropertiesConfiguration): String = {

    TokenReplaceRegex.replaceAllIn(string, m => s"${properties.getString(m.group(1))}")
  }
}

object StringInterpolator {

  protected final val TokenReplaceRegex: Regex = "\\$\\{([\\w.]+)}".r
}
