package it.luca.aurore.configuration.metadata.transform

import com.fasterxml.jackson.annotation.JsonProperty
import it.luca.aurore.configuration.Dto

case class Transform(@JsonProperty(Transform.Filters) filters: List[String],
                     @JsonProperty(Transform.Transformations) transformations: List[String],
                     dropDuplicates: Option[List[String]],
                     dropColumns: Option[List[String]],
                     partitioning: Partitioning)
  extends Dto {

  requiredNotEmpty(filters, Transform.Filters)
  requiredNotEmpty(transformations, Transform.Transformations)

}

object Transform {

  final val Filters = "filters"
  final val Transformations = "transformations"
}
