package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.StringValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.date_format

case class DateFormat(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    val pattern: String = getFunctionParameter[StringValue, String](1, _.getValue)
    date_format(column, pattern)
  }
}
