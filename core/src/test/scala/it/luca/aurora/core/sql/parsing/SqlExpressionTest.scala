package it.luca.aurora.core.sql.parsing

import org.apache.spark.sql.{Column, DataFrame, Encoder, Row, SparkSession}
import org.scalatest.matchers.should

/**
 * Trait for testing a correct parsing of SQL expressions that should transform data from one type to another
 * according to a given [[Function]]
 * @tparam I type of input data
 * @tparam O type of output data
 */

trait SqlExpressionTest[I, O]
  extends should.Matchers {

  protected def inputSampleToExpectedValue(input: I): O

  protected def rowToActualValue(r: Row): O

  def test(expression: String, inputSamples: Seq[I])
          (implicit sparkSession: SparkSession, evidence$1: Encoder[I]): Unit = {

    import sparkSession.implicits._

    val testDf: DataFrame = inputSamples
      .toDF(SqlExpressionTest.firstColumnName)
      .coalesce(1)

    val newColumn: Column = SqlExpressionParser.parse(expression)

    val expectedValues: Seq[O] = inputSamples.map(inputSampleToExpectedValue)
    val actualValues: Seq[O] = testDf.select(newColumn).collect().map(rowToActualValue).toSeq
    actualValues.zip(expectedValues) foreach {
      case (actual, expected) =>
        actual shouldEqual expected
    }
  }
}

object SqlExpressionTest {

  final val firstColumnName = "first_column"
  final val secondColumnName = "second_column"
}
