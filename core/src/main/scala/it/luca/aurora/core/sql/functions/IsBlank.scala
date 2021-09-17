package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{length, trim}

case class IsBlank(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = length(trim(column)) === 0
}
