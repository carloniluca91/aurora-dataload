package it.luca.aurora.core.utils

import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.ex.ConfigurationException

import java.io.File

object Utils {

  /**
   * Load given .properties file
   *
   * @param fileName name of .properties file
   * @return instance of [[org.apache.commons.configuration2.PropertiesConfiguration]]
   * @throws ConfigurationException if case of issues
   */

  @throws(classOf[ConfigurationException])
  def readProperties(fileName: String): PropertiesConfiguration = {

    val builder: FileBasedConfigurationBuilder[PropertiesConfiguration] = new FileBasedConfigurationBuilder(classOf[PropertiesConfiguration])
      .configure(new Parameters().fileBased()
        .setThrowExceptionOnMissing(true)
        .setListDelimiterHandler(new DefaultListDelimiterHandler(','))
        .setFile(new File(fileName)))

    builder.getConfiguration
  }

}
