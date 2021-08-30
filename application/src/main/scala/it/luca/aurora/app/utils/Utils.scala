package it.luca.aurora.app.utils

import it.luca.aurora.core.configuration.yaml.ApplicationYaml

import scala.util.matching.Regex

object Utils {

  def interpolateString(string: String, yaml: ApplicationYaml): String = {

    val regex: Regex = "\\$\\{([\\w.]+)}".r
    regex.replaceAllIn(string, m => s"${yaml.getProperty(m.group(1))}")
  }
}
