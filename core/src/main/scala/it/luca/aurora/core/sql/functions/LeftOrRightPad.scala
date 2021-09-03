package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.{LongValue, StringValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lpad, rpad}

case class LeftOrRightPad(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    val length: Int = getFunctionParameter[LongValue, Int](1, _.getValue.toInt)
    val padding: String = getFunctionParameter[StringValue, String](2, _.getValue)
    val padFunction: Column => Column = functionNameLowerCase match {
      case FunctionName.LeftPad => lpad(_, length, padding)
      case FunctionName.RightPad => rpad(_, length, padding)
    }

    padFunction(column)
  }
}
