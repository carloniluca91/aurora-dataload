package it.luca.aurora.configuration.metadata.transform

import com.fasterxml.jackson.annotation.JsonProperty
import it.luca.aurora.configuration.metadata.transform.FileNameRegexPartitioning._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

/**
 * Specifications for partitioning data according to the date embedded in the name of input data file
 *
 * @param partitioningType type of partitioning strategy
 * @param columnName name to assign to partitioning column
 * @param regexGroup regex group from which embedded date should be extracted
 * @param inputPattern pattern of embedded date
 * @param outputPattern pattern of embedded date on partition column
 */

case class FileNameRegexPartitioning(@JsonProperty(Partitioning.Type) override val partitioningType: String,
                                     @JsonProperty(Partitioning.ColumnName) override val columnName: String,
                                     @JsonProperty(RegexGroup) regexGroup: Int,
                                     @JsonProperty(InputPattern) inputPattern: String,
                                     @JsonProperty(OutputPattern) outputPattern: String)
  extends Partitioning(partitioningType, columnName) {

  required(regexGroup, "regexGroup")
  required(inputPattern,"inputPattern")
  required(outputPattern,"outputPattern")

  /**
   * Extract and parse a date from a string representing a file name
   * @param fileNameRegex regex to be used for date extraction
   * @param fileName name of the file
   * @throws IllegalArgumentException if matching fails
   * @return extracted date
   */

  @throws[IllegalArgumentException]
  def getDateFromFileName(fileNameRegex: Regex, fileName: String): String = {

    fileNameRegex.findFirstMatchIn(fileName) match {
      case Some(value) => LocalDate
        .parse(value.group(regexGroup), DateTimeFormatter.ofPattern(inputPattern))
        .format(DateTimeFormatter.ofPattern(outputPattern))
      case None => throw new IllegalArgumentException(s"Given file name ($fileName) does not macth given regex ($fileNameRegex)")
    }
  }
}

object FileNameRegexPartitioning {

  final val RegexGroup = "regexGroup"
  final val InputPattern = "inputPattern"
  final val OutputPattern = "outputPattern"
}