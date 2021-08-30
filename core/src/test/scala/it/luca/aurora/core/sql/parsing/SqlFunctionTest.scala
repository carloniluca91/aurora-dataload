package it.luca.aurora.core.sql.parsing

import org.apache.spark.sql.{Column, DataFrame, Encoder, Row, SparkSession}
import org.scalatest.matchers.should

trait SqlFunctionTest[I, O]
  extends should.Matchers {

  protected val inputSampleToExpectedValue: I => O

  protected val rowToActualValue: Row => O

  def test(expression: String, inputSamples: Seq[I])
          (implicit sparkSession: SparkSession, evidence$1: Encoder[I]): Unit = {

    import sparkSession.implicits._

    val testDf: DataFrame = inputSamples
      .toDF(SqlFunctionTest.firstColumnName)
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

object SqlFunctionTest {

  final val firstColumnName = "first_column"
  final val secondColumnName = "second_column"
}
