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
  new JsonSubTypes.Type(value = classOf[FileNamePartitioning], name = Partitioning.FileName),
  new JsonSubTypes.Type(value = classOf[ColumnPartitioning], name = Partitioning.Column)))
abstract class Partitioning(val partitioningType: String,
                            val columnName: String)
  extends Dto {

  required(partitioningType, Partitioning.Type)
  required(columnName, Partitioning.ColumnName)
}

object Partitioning {

  final val Column = "column"
  final val FileName = "fileName"

  final val Type = "type"
  final val ColumnName = "columnName"
}