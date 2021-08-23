package it.luca.aurora.app.utils

import it.luca.aurora.core.configuration.yaml.ApplicationYaml

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

object Utils {

  def interpolateString(string: String, yaml: ApplicationYaml): String = {

    val regex: Regex = "\\$\\{([\\w.]+)}".r
    regex.replaceAllIn(string, m => s"${yaml.getProperty(m.group(1))}")
  }

  /**
   * Converts a [[java.sql.Timestamp]] to a [[scala.Predef.String]] with given pattern
   * @param ts input timestamp
   * @param pattern pattern for output string
   * @return input timestamp as a string with given pattern
   */

  def timestampToString(ts: Timestamp, pattern: String): String = {

    ts.toLocalDateTime
      .format(DateTimeFormatter.ofPattern(pattern))
  }
}
