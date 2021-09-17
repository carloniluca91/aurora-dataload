package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lower, when}

case class IsFlag(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    when(column.isNotNull, lower(column).isin("y", "n")).otherwise(false)
  }
}
