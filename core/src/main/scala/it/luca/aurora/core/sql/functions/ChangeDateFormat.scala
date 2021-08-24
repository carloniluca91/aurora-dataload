package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.StringValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{date_format, to_date}

case class ChangeDateFormat(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def getColumn(column: Column): Column = {

    val inputPattern: String = getFunctionParameter[StringValue, String](0, _.getValue)
    val outputPattern: String = getFunctionParameter[StringValue, String](1, _.getValue)
    date_format(to_date(column, inputPattern), outputPattern)
  }
}
