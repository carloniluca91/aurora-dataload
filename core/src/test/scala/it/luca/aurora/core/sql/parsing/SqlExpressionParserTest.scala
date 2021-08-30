package it.luca.aurora.core.sql.parsing

import it.luca.aurora.core.sql.functions.{ChangeDateFormat, FunctionName, MatchesDateOrTimestampFormat, ToDateOrTimestamp}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.util.Try

class SqlExpressionParserTest
  extends AnyFlatSpec
    with should.Matchers
    with BeforeAndAfterAll {

  protected implicit val sparkSession: SparkSession = SparkSession.builder()
    .master("local")
    .appName(s"${classOf[SqlExpressionParserTest].getSimpleName}")
    .getOrCreate()

  import sparkSession.implicits._

  override def afterAll(): Unit = {

    sparkSession.stop()
    super.afterAll()
  }

  s"The SQL parser" should s"parse an aliasing expression" in {

    val expression = s"${SqlExpressionTest.firstColumnName} as ${SqlExpressionTest.secondColumnName}"
    val testDf: DataFrame = ("hello" :: Nil).toDF(SqlExpressionTest.firstColumnName)
    val column: Column = SqlExpressionParser.parse(expression)
    testDf.select(column).columns.head shouldEqual SqlExpressionTest.secondColumnName
  }

  it should s"parse a ${classOf[ChangeDateFormat].getSimpleName} function" in {

    val (inputPattern, outputPattern) = ("yyyyMMdd", "yyyy-MM-dd")
    val (inputFormatter, outputFormatter) = (DateTimeFormatter.ofPattern(inputPattern), DateTimeFormatter.ofPattern(outputPattern))
    val expression = s"${FunctionName.ChangeDateFormat}(${SqlExpressionTest.firstColumnName}, '$inputPattern', '$outputPattern')"
    val functionTest: SqlExpressionTest[String, String] = new SqlExpressionTest[String, String] {
      override protected def inputSampleToExpectedValue(input: String): String = LocalDate.parse(input, inputFormatter).format(outputFormatter)
      override protected def rowToActualValue(r: Row): String = r.getString(0)
    }

    val minusDays: Int => String = days => LocalDate.now().minusDays(days).format(inputFormatter)
    val inputSamples: Seq[String] = minusDays(1) :: minusDays(0) :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[MatchesDateOrTimestampFormat].getSimpleName} function (date case)" in {

    val datePattern = "yyyy-MM-dd"
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePattern)
    val expression = s"${FunctionName.MatchesDateFormat}(${SqlExpressionTest.firstColumnName}, '$datePattern')"
    val functionTest: SqlExpressionTest[String, Boolean] = new SqlExpressionTest[String, Boolean] {
      override protected def inputSampleToExpectedValue(input: String): Boolean = Try { LocalDate.parse(input, formatter) }.isSuccess
      override protected def rowToActualValue(r: Row): Boolean = r.getBoolean(0)
    }

    val formatWithPattern: String => String = pattern => LocalDate.now().format(DateTimeFormatter.ofPattern(pattern))
    val inputSamples: Seq[String] = formatWithPattern(datePattern) :: formatWithPattern("yyyy/MM/dd") :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[MatchesDateOrTimestampFormat].getSimpleName} function (timestamp case)" in {

    val timestampPattern = "yyyy-MM-dd HH:mm:ss"
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(timestampPattern)
    val expression = s"${FunctionName.MatchesTimestampFormat}(${SqlExpressionTest.firstColumnName}, '$timestampPattern')"
    val functionTest: SqlExpressionTest[String, Boolean] = new SqlExpressionTest[String, Boolean] {
      override protected def inputSampleToExpectedValue(input: String): Boolean = Try { LocalDateTime.parse(input, formatter) }.isSuccess
      override protected def rowToActualValue(r: Row): Boolean = r.getBoolean(0)
    }

    val formatWithPattern: String => String = pattern => LocalDateTime.now().format(DateTimeFormatter.ofPattern(pattern))
    val inputSamples: Seq[String] = formatWithPattern(timestampPattern) :: formatWithPattern("yyyy/MM/dd HH:mm:ss") :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[ToDateOrTimestamp].getSimpleName} function (date case)" in {

    val datePattern = "yyyy-MM-dd"
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePattern)
    val expression = s"${FunctionName.ToDate}(${SqlExpressionTest.firstColumnName}, '$datePattern')"
    val functionTest: SqlExpressionTest[String, Date] = new SqlExpressionTest[String, Date] {
      override protected def inputSampleToExpectedValue(input: String): Date = Date.valueOf(LocalDate.parse(input, formatter))
      override protected def rowToActualValue(r: Row): Date = r.getDate(0)
    }

    val minusDays: Int => String = days => LocalDate.now().minusDays(days).format(formatter)
    val inputSamples: Seq[String] = minusDays(1) :: minusDays(0):: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[ToDateOrTimestamp].getSimpleName} function (timestamp case)" in {

    val timestampPattern = "yyyy-MM-dd HH:mm:ss"
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(timestampPattern)
    val expression = s"${FunctionName.ToTimestamp}(${SqlExpressionTest.firstColumnName}, '$timestampPattern')"
    val functionTest: SqlExpressionTest[String, Timestamp] = new SqlExpressionTest[String, Timestamp] {
      override protected def inputSampleToExpectedValue(input: String): Timestamp = Timestamp.valueOf(LocalDateTime.parse(input,formatter))
      override protected def rowToActualValue(r: Row): Timestamp = r.getTimestamp(0)
    }

    val minusMinutes: Int => String = minutes => LocalDateTime.now().minusMinutes(minutes).format(formatter)
    val inputSamples: Seq[String] = minusMinutes(5) :: minusMinutes(0) :: Nil
    functionTest.test(expression, inputSamples)
  }
}
