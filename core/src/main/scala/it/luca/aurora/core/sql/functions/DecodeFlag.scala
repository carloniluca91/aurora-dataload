package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression

case class DecodeFlag(override protected val function: expression.Function)
  extends CaseWhenFunction[String, Boolean](function) {

  override protected val casesMap: Map[String, Boolean] = functionNameLowerCase match {
    case FunctionName.DecodeBinaryFlag => Map("0" -> false, "1" -> true)
    case FunctionName.DecodeYNFlag => Map("Y" -> true, "N" -> false)
  }
}
