package it.luca.aurora.app.job.transformer

import it.luca.aurora.configuration.metadata.transform.ColumnExpressionPartitioning
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.spark.sql.Column

object ColumnExpressionPartitionComputer
  extends PartitionColumnComputer[ColumnExpressionPartitioning] {

  override def getPartitionColumn(input: ColumnExpressionPartitioning): Column = {

    val columnExpression: String = input.columnExpression
    val partitionColumn: Column = SqlExpressionParser.parse(columnExpression)
    log.info(s"Successfully converted given column expression ($columnExpression) to a ${classOf[Column].getSimpleName}")
    partitionColumn
  }
}
