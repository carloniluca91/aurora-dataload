package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression.{Expression, Function}
import org.apache.spark.sql.Column

/**
 * Base class for implementing a SQL function
 * @param function instance of [[Function]]
 */

sealed abstract class SqlFunction(protected val function: Function) {

  protected final val functionName: String = function.getName
  protected final def getFunctionParameter[R <: Expression, T](index: Int, parameterConversion: R => T): T = {

    val nthParameterExpression: R = function.getParameters
      .getExpressions.get(index)
      .asInstanceOf[R]

    parameterConversion(nthParameterExpression)
  }
}

/**
 * Base class for implementing a SQL function taking a single [[Column]] as input
 * @param function instance of [[Function]]
 */

abstract class SingleColumnFunction(override protected val function: Function)
  extends SqlFunction(function) {

  /**
   * Transform given column
   * @param column input [[Column]]
   * @return input column transformed by this SQL functions
   */

  def transform(column: Column): Column
}

/**
 * Base class for implementing a SQL function taking multiple [[Column]](s) as input
 * @param function instance of [[Function]]
 */

abstract class MultipleColumnFunction(override protected val function: Function)
  extends SqlFunction(function) {

  /**
   * Compute output column depending on multiple input columns
   * @param columns list of [[Column]]
   * @return column computed by applying this SQL function on input columns
   */

  def transform(columns: Column*): Column

}