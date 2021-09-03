package it.luca.aurora.core.sql.parsing

import it.luca.aurora.core.sql.parsing.SqlExpressionTest.firstColumnName
import org.apache.spark.sql.{Column, DataFrame, Encoder, Row, SparkSession}
import org.scalatest.matchers.should

/**
 * Trait for testing the correct parsing of a SQL expression
 * @tparam I type of input data
 * @tparam O type of output data
 */

trait SqlExpressionTest[I, O]
  extends should.Matchers {

  /**
   * Compute expected value from an instance of [[I]]
   * @param input instance of [[I]]
   * @return transformed input
   */

  protected def computeExpectedValue(input: I): O

  /**
   * Get the actual value computed by the SQL expression
   * @param r instance of [[Row]]
   * @return instance of [[O]]
   */

  protected def getActualValue(r: Row): O

  /**
   * Test parsing and effective definition of SQL expression involving a single column
   * @param expression input SQL expression
   * @param inputSamples list of input samples
   * @param sparkSession implicit [[SparkSession]] (used for creating test [[DataFrame]] from input samples)
   * @param evidence$1 implicit [[Encoder]] for type [[I]]
   */

  def test(expression: String, inputSamples: Seq[I])
          (implicit sparkSession: SparkSession, evidence$1: Encoder[I]): Unit = {

    test(expression, inputSamples, firstColumnName :: Nil)
  }

  /**
   * Test parsing and effective definition of a SQL expression involving multiple columns
   * @param expression input SQL expression
   * @param inputSamples list of input samples
   * @param dataFrameColumnNames column names for underlying test [[DataFrame]]
   * @param sparkSession implicit [[SparkSession]] (used for creating test [[DataFrame]] from input samples)
   * @param evidence$1 implicit [[Encoder]] for type [[I]]
   */

  def test(expression: String, inputSamples: Seq[I], dataFrameColumnNames: Seq[String])
          (implicit sparkSession: SparkSession, evidence$1: Encoder[I]): Unit = {

    import sparkSession.implicits._

    val testDf: DataFrame = inputSamples
      .toDF(dataFrameColumnNames: _*)
      .coalesce(1)

    val newColumn: Column = SqlExpressionParser.parse(expression)

    val expectedValues: Seq[O] = inputSamples.map(computeExpectedValue)
    val actualValues: Seq[O] = testDf.select(newColumn).collect().map(getActualValue).toSeq
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
