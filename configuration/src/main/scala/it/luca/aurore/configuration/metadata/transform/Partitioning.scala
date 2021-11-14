package it.luca.aurore.configuration.metadata.transform

import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import it.luca.aurore.configuration.Dto

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
  property = Partitioning.Type,
  visible = true)
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[FileNameRegexPartitioning], name = Partitioning.FileNameRegex),
  new JsonSubTypes.Type(value = classOf[ColumnPartitioning], name = Partitioning.ColumnName)))
sealed abstract class Partitioning(protected val partitioningType: String,
                                   protected val columnName: String)
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

case class FileNameRegexPartitioning(@JsonProperty(Partitioning.Type) override protected val partitioningType: String,
                                     @JsonProperty(Partitioning.ColumnName) override protected val columnName: String,
                                     regexGroup: Int,
                                     inputPattern: String,
                                     outputPattern: String)
  extends Partitioning(partitioningType, columnName) {

  required(regexGroup, "regexGroup")
  required(inputPattern,"inputPattern")
  required(outputPattern,"outputPattern")
}

case class ColumnPartitioning(@JsonProperty(Partitioning.Type) override protected val partitioningType: String,
                              @JsonProperty(Partitioning.ColumnName) override protected val columnName: String,
                              columnExpression: String)
  extends Partitioning(partitioningType, columnName) {

  required(columnExpression, "columnExpression")
}