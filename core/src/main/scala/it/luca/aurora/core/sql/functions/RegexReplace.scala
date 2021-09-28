package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.StringValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_replace

case class RegexReplace(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    val pattern: String = getFunctionParameter[StringValue, String](1, _.getValue)
    val replacement: String = getFunctionParameter[StringValue, String](2, _.getValue)
    regexp_replace(column, pattern, replacement)
  }
}
