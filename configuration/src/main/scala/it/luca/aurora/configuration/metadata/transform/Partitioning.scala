package it.luca.aurora.configuration.metadata.transform

import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import it.luca.aurora.configuration.Dto

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
  property = Partitioning.Type,
  visible = true)
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[FileNameRegexPartitioning], name = Partitioning.FileNameRegex),
  new JsonSubTypes.Type(value = classOf[ColumnPartitioning], name = Partitioning.ColumnName)))
sealed abstract class Partitioning(val partitioningType: String,
                                   val columnName: String)
  extends Dto {

  required(partitioningType, Partitioning.Type)
  required(columnName, Partitioning.ColumnName)
}

object Partitioning {

  final val Column = "column"
  final val ColumnName = "columnName"

  final val FileNameRegex = "fileNameRegex"
  final val Type = "type"
}

case class FileNameRegexPartitioning(@JsonProperty(Partitioning.Type) override val partitioningType: String,
                                     @JsonProperty(Partitioning.ColumnName) override val columnName: String,
                                     regexGroup: Int,
                                     inputPattern: String,
                                     outputPattern: String)
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

case class ColumnPartitioning(@JsonProperty(Partitioning.Type) override val partitioningType: String,
                              @JsonProperty(Partitioning.ColumnName) override val columnName: String,
                              columnExpression: String)
  extends Partitioning(partitioningType, columnName) {

  required(columnExpression, "columnExpression")
}