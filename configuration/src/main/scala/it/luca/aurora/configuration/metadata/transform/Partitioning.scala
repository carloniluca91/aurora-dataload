package it.luca.aurora.configuration.metadata.transform

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import it.luca.aurora.configuration.Dto

/**
 * Specifications for computing data partitioning column
 * @param partitioningType type of partitioning strategy
 * @param columnName name to assign to partitioning column
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
  property = Partitioning.Type,
  visible = true)
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[FileNameRegexPartitioning], name = Partitioning.FileNameRegex),
  new JsonSubTypes.Type(value = classOf[ColumnPartitioning], name = Partitioning.ColumnName)))
abstract class Partitioning(val partitioningType: String,
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