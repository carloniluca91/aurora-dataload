package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.operators.relational.ExpressionList
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should

abstract class SqlExpressionTest[T, R]
  extends should.Matchers
    with MockFactory {

  protected val functionName: String
  protected val parametersExpression: java.util.List[Expression]
  protected val expectedValuesFunction: Row => R
  protected val createSingleColumnFunction: expression.Function => SingleColumnFunction

  protected final val function: expression.Function = stub[expression.Function]
  protected final val functionParameters: ExpressionList = stub[ExpressionList]
  (functionParameters.getExpressions _).when().returns(parametersExpression)

  (function.getName _).when().returns(functionName)
  (function.getParameters _).when().returns(functionParameters)

  protected def test(testDf: DataFrame): Unit = {

    val expectedValues: Seq[R] = testDf.collect().map(expectedValuesFunction).toSeq
    val singleColumnFunction: SingleColumnFunction = createSingleColumnFunction(function)
    val actualValues: Seq[R] = testDf.select(singleColumnFunction.getColumn(col(testDf.columns.head)))
      .collect().map(r => r.getAs[R](0))

    actualValues.zip(expectedValues) foreach {
      case (actual, expected) => actual shouldEqual expected
    }
  }
}
