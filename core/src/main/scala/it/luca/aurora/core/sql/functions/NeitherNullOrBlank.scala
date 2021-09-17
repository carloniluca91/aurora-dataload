package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{length, trim, when}

case class NeitherNullOrBlank(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    when(column.isNotNull, length(trim(column)) =!= 0)
      .otherwise(false)
  }
}
