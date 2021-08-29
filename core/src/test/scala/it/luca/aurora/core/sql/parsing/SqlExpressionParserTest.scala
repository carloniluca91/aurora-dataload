package it.luca.aurora.core.sql.parsing

import it.luca.aurora.core.sql.functions.{FunctionName, ToDateOrTimestamp}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SqlExpressionParserTest
  extends AnyFlatSpec
    with should.Matchers {

  private val (firstColStr, secondColStr) = ("c1", "c2")
  private val sparkSession: SparkSession = SparkSession.builder()
    .master("local")
    .appName(s"${classOf[SqlExpressionParserTest].getSimpleName}")
    .getOrCreate()

  s"The SQL parser" should s"parse a ${classOf[ToDateOrTimestamp].getSimpleName} function" in {

    import sparkSession.implicits._

    val (datePattern, timestampPattern) = ("yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")
    val expression = s"${FunctionName.ToDate}($firstColStr, '$datePattern')"
    val expectedValue = LocalDate.now()
    val testDfDate: DataFrame = (expectedValue
      .format(DateTimeFormatter.ofPattern(datePattern)) :: Nil).toDF(firstColStr)

    val parsedColumn: Column = SqlExpressionParser.parse(expression)
    val actualValue: LocalDate = testDfDate.withColumn(secondColStr, parsedColumn)
      .select(secondColStr).collect()
      .map(r => r.getDate(0).toLocalDate).head

    actualValue shouldEqual expectedValue
  }
}
