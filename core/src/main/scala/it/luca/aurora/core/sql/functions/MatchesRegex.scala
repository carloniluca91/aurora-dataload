package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.StringValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when

case class MatchesRegex(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    val regex: String = getFunctionParameter[StringValue, String](1, _.getValue)
    when(column.isNotNull, column.rlike(regex)).otherwise(false)
  }
}
