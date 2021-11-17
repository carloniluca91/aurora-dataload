package it.luca.aurora.app.job.transformer

import it.luca.aurora.configuration.metadata.transform.ColumnExpressionPartitioning
import it.luca.aurora.core.Logging
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.spark.sql.Column

/**
 * Partition column computer based on a Sql expression
 * @param partitioning instance of [[ColumnExpressionPartitioning]]
 */

class ColumnExpressionPartitionComputer(override protected val partitioning: ColumnExpressionPartitioning)
  extends PartitionColumnComputer[ColumnExpressionPartitioning](partitioning)
    with Logging {

  override def getPartitionColumn: Column = {

    val columnExpression: String = partitioning.columnExpression
    val partitionColumn: Column = SqlExpressionParser.parse(columnExpression)
    log.info(s"Successfully converted given column expression ($columnExpression) to a ${classOf[Column].getSimpleName}")
    partitionColumn
  }
}
