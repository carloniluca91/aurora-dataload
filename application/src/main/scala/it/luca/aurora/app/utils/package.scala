package it.luca.aurora.app

import it.luca.aurora.core.Logging
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.ex.ConfigurationException

import java.io.File

package object utils
  extends Logging{

  /**
   * Load given .properties file
   * @param fileName name of .properties file
   * @return instance of [[PropertiesConfiguration]]
   * @throws ConfigurationException if case of issues
   */

  @throws[ConfigurationException]
  def loadProperties(fileName: String): PropertiesConfiguration = {

    val builder = new FileBasedConfigurationBuilder[PropertiesConfiguration](classOf[PropertiesConfiguration])
      .configure(new Parameters().fileBased
        .setThrowExceptionOnMissing(true)
        .setListDelimiterHandler(new DefaultListDelimiterHandler(',')).setFile(new File(fileName)))

    val properties: PropertiesConfiguration = builder.getConfiguration
    log.info(s"Successfully loaded .properties file $fileName")
    properties
  }
}
