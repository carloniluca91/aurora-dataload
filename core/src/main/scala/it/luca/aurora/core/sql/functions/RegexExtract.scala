package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.{LongValue, StringValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_extract

case class RegexExtract(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    val regexString: String = getFunctionParameter[StringValue, String](1, _.getValue)
    val groupIndex: Int = getFunctionParameter[LongValue, Int](2, _.getValue.toInt)
    regexp_extract(column, regexString, groupIndex)
  }
}
