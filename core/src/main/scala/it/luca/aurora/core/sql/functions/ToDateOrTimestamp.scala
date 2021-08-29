package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.StringValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_date, to_timestamp}

case class ToDateOrTimestamp(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def getColumn(column: Column): Column = {

    val pattern: String = getFunctionParameter[StringValue, String](1, _.getValue)
    val timeFunction: Column => Column = if (functionName.toLowerCase.endsWith("date"))
      to_date(_, pattern) else
      to_timestamp(_, pattern)
    timeFunction(column)
  }
}
