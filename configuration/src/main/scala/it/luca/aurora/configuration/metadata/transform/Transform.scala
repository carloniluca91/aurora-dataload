package it.luca.aurora.configuration.metadata.transform

import com.fasterxml.jackson.annotation.JsonProperty
import it.luca.aurora.configuration.Dto
import it.luca.aurora.core.Logging
import org.apache.spark.sql.DataFrame

/**
 * Specifications for data transformations
 * @param filters Sql expressions representing filters to apply on input data
 * @param transformations Sql expressions representing transformations to apply on input data
 * @param dropDuplicates optional column names to be used for duplicates computation and removal
 * @param dropColumns optional column names to drop
 * @param partitioning instance of [[Partitioning]]
 */

case class Transform(@JsonProperty(Transform.Filters) filters: List[String],
                     @JsonProperty(Transform.Transformations) transformations: List[String],
                     dropDuplicates: Option[List[String]],
                     dropColumns: Option[List[String]],
                     partitioning: Partitioning)
  extends Dto
    with Logging {

  requiredNotEmpty(filters, Transform.Filters)
  requiredNotEmpty(transformations, Transform.Transformations)

  /**
   * Remove duplicates and drop columns from a given [[DataFrame]]
   * @param dataFrame input dataFrame
   * @return input dataFrame with
   */

  def maybeDropDuplicatesAndColumns(dataFrame: DataFrame): DataFrame = {

    val dataFrameClassName: String = classOf[DataFrame].getSimpleName
    val describe: Seq[String] => String = seq => seq.map(x => s"  $x").mkString("\n").concat("\n")
    val dataFrameMaybeWithDroppedDuplicates: DataFrame = dropDuplicates match {
      case Some(value) =>
        log.info(s"Dropping duplicates from given $dataFrameClassName computed along columns\n\n${describe(value)}")
        dataFrame.dropDuplicates(value)
      case None =>
        log.info(s"No duplicates will be removed from given $dataFrameClassName")
        dataFrame
    }

    dropColumns match {
      case Some(value) =>
        log.info(s"Dropping following columns from given $dataFrameClassName\n\n${describe(value)}")
        dataFrameMaybeWithDroppedDuplicates.drop(value: _*)
      case None =>
        log.info(s"No columns to remove from given $dataFrameClassName")
        dataFrameMaybeWithDroppedDuplicates
    }
  }
}

object Transform {

  final val Filters = "filters"
  final val Transformations = "transformations"
}
