package it.luca.aurora.configuration.metadata.transform

import com.fasterxml.jackson.annotation.JsonProperty
import it.luca.aurora.configuration.Dto

/**
 * Specifications for data transformations
 * @param filters Sql expressions representing filters to apply on input data
 * @param transformations Sql expressions representing transformations to apply on input data
 * @param dropDuplicates optional column names to be used for duplicates computation and removal
 * @param dropColumns optional column names to drop
 * @param partitioning instance of [[Partitioning]]
 */

case class Transform(@JsonProperty(Transform.Filters) filters: Seq[String],
                     @JsonProperty(Transform.Transformations) transformations: Seq[String],
                     dropDuplicates: Option[Seq[String]],
                     dropColumns: Option[Seq[String]],
                     partitioning: Partitioning)
  extends Dto {

  requiredNotEmpty(filters, Transform.Filters)
  requiredNotEmpty(transformations, Transform.Transformations)
}

object Transform {

  final val Filters = "filters"
  final val Transformations = "transformations"
}
