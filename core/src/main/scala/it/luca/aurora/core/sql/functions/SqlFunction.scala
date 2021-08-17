package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression.{Expression, Function}
import org.apache.spark.sql.Column

sealed abstract class SqlFunction(protected val function: Function) {

  protected final val functionName: String = function.getName
  protected final def getFunctionParameter[R <: Expression, T](index: Int, parameterConversion: R => T) :T = {

    val nThParameterExpression: R = function.getParameters.getExpressions.get(index).asInstanceOf[R]
    parameterConversion(nThParameterExpression)
  }
}

abstract class SingleColumnFunction(override protected val function: Function)
  extends SqlFunction(function) {

  def getColumn(column: Column): Column
}

abstract class MultipleColumnFunction(override protected val function: Function)
  extends SqlFunction(function) {

  def getColumn(columns: Column*): Column

}