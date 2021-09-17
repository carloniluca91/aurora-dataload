package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{ltrim, rtrim, trim}

case class LeftOrRightOrBothTrim(override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  override def transform(column: Column): Column = {

    val trimFunction: Column => Column = functionNameLowerCase match {
      case FunctionName.LeftTrim => ltrim
      case FunctionName.RightTrim => rtrim
      case FunctionName.Trim => trim
    }

    trimFunction(column)
  }
}
