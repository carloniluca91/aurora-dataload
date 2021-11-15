package it.luca.aurora.configuration.metadata.transform

import com.fasterxml.jackson.annotation.JsonProperty
import it.luca.aurora.core.Logging
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.spark.sql.Column

/**
 * Specifications for partitioning data using a column expression
 *
 * @param partitioningType type of partitioning strategy
 * @param columnName name to assign to partitioning column
 * @param columnExpression Sql expression representing partition column to generate
 */

case class ColumnExpressionPartitioning(@JsonProperty(Partitioning.Type) override val partitioningType: String,
                                        @JsonProperty(Partitioning.ColumnName) override val columnName: String,
                                        @JsonProperty(ColumnExpressionPartitioning.ColumnExpression) columnExpression: String)
  extends Partitioning(partitioningType, columnName)
    with Logging {

  required(columnExpression, "columnExpression")

  /**
   * Get partition column
   * @return instance of [[Column]] to used for data partitioning
   */

  def getPartitionColumn: Column = {

    val column: Column = SqlExpressionParser.parse(columnExpression)
    log.info(s"Successfully parsed partitioning expression from ${classOf[ColumnExpressionPartitioning].getSimpleName}")
    column
  }
}

object ColumnExpressionPartitioning {

  final val ColumnExpression = "columnExpression"
}
