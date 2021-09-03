package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.StringValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat_ws

case class ConcatWs(override protected val function: expression.Function)
  extends MultipleColumnFunction(function) {

  override def transform(columns: Column*): Column = {

    val separator: String = getFunctionParameter[StringValue, String](0, _.getValue)
    concat_ws(separator, columns: _*)
  }
}
