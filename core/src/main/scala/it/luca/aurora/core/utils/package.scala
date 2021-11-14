package it.luca.aurora.core

import org.apache.commons.configuration2.PropertiesConfiguration

import scala.io.{BufferedSource, Source}

package object utils
  extends Logging {

  /**
   * Read given .properties file
   * @param fileName name of .properties file
   * @return instance of [[PropertiesConfiguration]]
   */

  def loadPropertiesFile(fileName: String): PropertiesConfiguration = loadProperties(fileName)(s => Source.fromFile(s))

  /**
   * Read given .properties resource
   * @param resourceName name of .properties file
   * @return instance of [[PropertiesConfiguration]]
   */

  def loadPropertiesResource(resourceName: String): PropertiesConfiguration =
    loadProperties(resourceName)(s => Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream(s)))

  /**
   * Read a .properties file
   * @param input input name (file or resource)
   * @param function function to be applied on input (using [[Source]]'s methods)
   * @return instance of [[PropertiesConfiguration]]
   */

  protected def loadProperties(input: String)(function: String => BufferedSource): PropertiesConfiguration = {

    val properties = new PropertiesConfiguration
    properties.setThrowExceptionOnMissing(true)
    properties.read(function(input).reader())
    log.info(s"Successfully loaded .properties file $input")
    properties
  }

  /**
   * Replace all tokens within given string with related property's value.
   * I.e., given a [[PropertiesConfiguration]] instance holding a property named ''a.property'' whose value is ''hello'',
   * a string like "${a.property}" becomes ''"hello"''
   * @param str input string
   * @param properties instance of [[PropertiesConfiguration]] holding propertiy's value
   * @return input string with
   */

  def replaceTokensWithProperties(str: String, properties: PropertiesConfiguration): String = {

    "\\$\\{([\\w|.]+)}".r
      .replaceAllIn(str, rMatch => properties.getString(rMatch.group(1)))
  }
}
