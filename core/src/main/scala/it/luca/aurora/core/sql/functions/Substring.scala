package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.LongValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.substring

case class Substring(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    val pos: Int = getFunctionParameter[LongValue, Int](1, _.getValue.toInt)
    val len: Int = getFunctionParameter[LongValue, Int](2, _.getValue.toInt)
    substring(column, pos, len)
  }
}
