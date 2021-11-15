package it.luca.aurora.configuration.metadata.transform

import com.fasterxml.jackson.annotation.JsonProperty
import it.luca.aurora.configuration.metadata.transform.FileNamePartitioning._

/**
 * Specifications for partitioning data according to the date embedded in the name of input data file
 *
 * @param partitioningType type of partitioning strategy
 * @param columnName name to assign to partitioning column
 * @param regexGroup regex group from which embedded date should be extracted
 * @param inputPattern pattern of embedded date
 * @param outputPattern pattern of embedded date on partition column
 */

case class FileNamePartitioning(@JsonProperty(Partitioning.Type) override val partitioningType: String,
                                @JsonProperty(Partitioning.ColumnName) override val columnName: String,
                                @JsonProperty(RegexGroup) regexGroup: Int,
                                @JsonProperty(InputPattern) inputPattern: String,
                                @JsonProperty(OutputPattern) outputPattern: String)
  extends Partitioning(partitioningType, columnName) {

  required(regexGroup, RegexGroup)
  required(inputPattern,InputPattern)
  required(outputPattern,OutputPattern)
}

object FileNamePartitioning {

  final val RegexGroup = "regexGroup"
  final val InputPattern = "inputPattern"
  final val OutputPattern = "outputPattern"
}