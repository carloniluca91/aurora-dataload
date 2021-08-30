package it.luca.aurora.core.sql.parsing

import it.luca.aurora.core.sql.functions.{ChangeDateFormat, FunctionName, ToDateOrTimestamp}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

class SqlExpressionParserTest
  extends AnyFlatSpec
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

  s"The SQL parser" should s"parse a ${classOf[ChangeDateFormat].getSimpleName} function" in {

    val (inputPattern, outputPattern) = ("yyyyMMdd", "yyyy-MM-dd")
    val (inputFormatter, outputFormatter) = (DateTimeFormatter.ofPattern(inputPattern), DateTimeFormatter.ofPattern(outputPattern))
    val expression = s"${FunctionName.ChangeDateFormat}(${SqlFunctionTest.firstColumnName}, '$inputPattern', '$outputPattern')"
    val functionTest: SqlFunctionTest[String, String] = new SqlFunctionTest[String, String] {
      override protected val inputSampleToExpectedValue: String => String = s => LocalDate.parse(s, inputFormatter).format(outputFormatter)
      override protected val rowToActualValue: Row => String = r => r.getString(0)
    }

    val minusDays: Int => String = days => LocalDate.now().minusDays(days).format(inputFormatter)
    val inputSamples: Seq[String] = minusDays(1) :: minusDays(0) :: Nil
    functionTest.test(expression, inputSamples)
  }


  it should s"parse a ${classOf[ToDateOrTimestamp].getSimpleName} function (date case)" in {

    val datePattern = "yyyy-MM-dd"
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePattern)
    val expression = s"${FunctionName.ToDate}(${SqlFunctionTest.firstColumnName}, '$datePattern')"
    val functionTest: SqlFunctionTest[String, Date] = new SqlFunctionTest[String, Date] {
      override protected val inputSampleToExpectedValue: String => Date = s => Date.valueOf(LocalDate.parse(s, formatter))
      override protected val rowToActualValue: Row => Date = r => r.getDate(0)
    }

    val minusDays: Int => String = days => LocalDate.now().minusDays(days).format(formatter)
    val inputSamples: Seq[String] = minusDays(1) :: minusDays(0):: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[ToDateOrTimestamp].getSimpleName} function (timestamp case)" in {

    val timestampPattern = "yyyy-MM-dd HH:mm:ss"
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(timestampPattern)
    val expression = s"${FunctionName.ToTimestamp}(${SqlFunctionTest.firstColumnName}, '$timestampPattern')"
    val functionTest: SqlFunctionTest[String, Timestamp] = new SqlFunctionTest[String, Timestamp] {
      override protected val inputSampleToExpectedValue: String => Timestamp = s => Timestamp.valueOf(LocalDateTime.parse(s,formatter))
      override protected val rowToActualValue: Row => Timestamp = r => r.getTimestamp(0)
    }

    val minusMinutes: Int => String = minutes => LocalDateTime.now().minusMinutes(minutes).format(formatter)
    val inputSamples: Seq[String] = minusMinutes(5) :: minusMinutes(0) :: Nil
    functionTest.test(expression, inputSamples)
  }
}
