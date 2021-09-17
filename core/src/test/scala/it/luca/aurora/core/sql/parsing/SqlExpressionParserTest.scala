package it.luca.aurora.core.sql.parsing

import it.luca.aurora.core.sql.functions._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util
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

  it should s"parse a CAST expression" in {

    val expression = s"cast(${SqlExpressionTest.firstColumnName} as int)"
    val functionTest: SqlExpressionTest[String, Int] = new SqlExpressionTest[String, Int] {
      override protected def computeExpectedValue(input: String): Int = input.toInt
    }

    val inputSamples: Seq[String] = "01" :: "02" :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a IN expression" in {

    val inClauseValues: Seq[String] = "1" :: "2" :: Nil
    val expression = s"${SqlExpressionTest.firstColumnName} in (${inClauseValues.map(s => s"'$s'").mkString(", ")})"
    val functionTest: SqlExpressionTest[String, Boolean] = new SqlExpressionTest[String, Boolean] {
      override protected def computeExpectedValue(input: String): Boolean = inClauseValues.contains(input)
    }

    val inputSamples: Seq[String] = "1" :: "2" :: "3" :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[Concat].getSimpleName} function" in {

    val columnNames: Seq[String] = SqlExpressionTest.firstColumnName :: SqlExpressionTest.secondColumnName :: Nil
    val expression = s"${FunctionName.Concat}(${columnNames.mkString(", ")})"
    val functionTest: SqlExpressionTest[(String, String), String] = new SqlExpressionTest[(String, String), String] {
      override protected def computeExpectedValue(input: (String, String)): String = input._1.concat(input._2)
    }

    val inputSamples: Seq[(String, String)] = ("hello", "world") :: Nil
    functionTest.test(expression, inputSamples, columnNames)
  }

  it should s"parse a ${classOf[ConcatWs].getSimpleName} function" in {

    val separator = ","
    val columnNames: Seq[String] = SqlExpressionTest.firstColumnName :: SqlExpressionTest.secondColumnName :: Nil
    val expression = s"${FunctionName.ConcatWs}('$separator', ${columnNames.mkString(", ")})"
    val functionTest: SqlExpressionTest[(String, String), String] = new SqlExpressionTest[(String, String), String] {
      override protected def computeExpectedValue(input: (String, String)): String = input._1.concat(separator).concat(input._2)
    }

    val inputSamples: Seq[(String, String)] = ("hello", "world") :: Nil
    functionTest.test(expression, inputSamples, columnNames)
  }

  it should s"parse a ${classOf[DateFormat].getSimpleName} function" in {

    val pattern = "yyyy-MM-dd"
    val dtGeneratorF: Int => Date = i => Date.valueOf(LocalDate.now().minusDays(i))
    val expression = s"${FunctionName.DateFormat}(${SqlExpressionTest.firstColumnName}, '$pattern')"
    val functionTest: SqlExpressionTest[Date, String] = new SqlExpressionTest[Date, String] {
      override protected def computeExpectedValue(input: Date): String = input.toLocalDate.format(DateTimeFormatter.ofPattern(pattern))
    }

    val inputSamples: Seq[Date] = dtGeneratorF(2) :: dtGeneratorF(1) :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[LeftOrRightOrBothTrim].getSimpleName} function" in {

    val doubleSpace = "  "
    val samples: Seq[String] = "hello" :: "world" :: Nil
    val testSet: Map[String, (String => String, String => String)] = Map(
      FunctionName.LeftTrim -> (StringUtils.stripStart(_, null), doubleSpace.concat),
      FunctionName.RightTrim -> (StringUtils.stripEnd(_, null), s => s.concat(doubleSpace)),
      FunctionName.Trim -> (StringUtils.trim, s => doubleSpace.concat(s).concat(doubleSpace)))

    testSet.foreach {
      case (key, (expectedF, sampleGeneratorF)) =>
        val expression = s"$key(${SqlExpressionTest.firstColumnName})"
        val functionTest: SqlExpressionTest[String, String] = new SqlExpressionTest[String, String] {
          override protected def computeExpectedValue(input: String): String = expectedF(input)
        }

        val inputSamples: Seq[String] = samples.map(sampleGeneratorF)
        functionTest.test(expression, inputSamples)
    }
  }

  it should s"parse a ${classOf[LeftOrRightPad].getSimpleName} function" in {

    val (len, padding): (Int, String) = (10, "0")
    val testSet: Map[String, String => String] = Map(
      FunctionName.LeftPad -> (StringUtils.leftPad(_, len, padding)),
      FunctionName.RightPad -> (StringUtils.rightPad(_, len, padding))
    )

    testSet.foreach {
      case (key, expectedF) =>
        val expression = s"$key(${SqlExpressionTest.firstColumnName}, $len, '$padding')"
        val functionTest: SqlExpressionTest[String, String] = new SqlExpressionTest[String, String] {
          override protected def computeExpectedValue(input: String): String = expectedF(input)
        }

        val inputSamples: Seq[String] = "hello" :: "world" :: Nil
        functionTest.test(expression, inputSamples)
    }
  }

  it should s"parse a ${classOf[MatchesDateOrTimestampFormat].getSimpleName} function" in {

    val (dtPattern, tsPattern) = ("yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")
    val (dtFormatter, tsFormatter) = (DateTimeFormatter.ofPattern(dtPattern), DateTimeFormatter.ofPattern(tsPattern))
    val dtWithPattern: String => String = s => LocalDate.now().format(DateTimeFormatter.ofPattern(s))
    val tsWithPattern: String => String = s => LocalDateTime.now().format(DateTimeFormatter.ofPattern(s))

    val testSet: Map[String, (String, String => Boolean, String => String)] = Map(
      FunctionName.MatchesDateFormat -> (dtPattern, s => Try {
        LocalDate.parse(s, dtFormatter)
      }.isSuccess, dtWithPattern),
      FunctionName.MatchesTimestampFormat -> (tsPattern, s => Try {
        LocalDateTime.parse(s, tsFormatter)
      }.isSuccess, tsWithPattern))

    testSet.foreach {
      case (key, (pattern, expectedF, sampleGeneratorF)) =>
        val expression = s"$key(${SqlExpressionTest.firstColumnName}, '$pattern')"
        val functionTest: SqlExpressionTest[String, Boolean] = new SqlExpressionTest[String, Boolean] {
          override protected def computeExpectedValue(input: String): Boolean = expectedF(input)
        }

        val inputSamples: Seq[String] = sampleGeneratorF(pattern) :: sampleGeneratorF("dd/MM/yyyy") :: Nil
        functionTest.test(expression, inputSamples)
    }
  }

  it should s"parse a ${classOf[NeitherNullOrBlank].getSimpleName} function" in {

    val expression = s"${FunctionName.NeitherNullOrBlank}(${SqlExpressionTest.firstColumnName})"
    val functionTest: SqlExpressionTest[String, Boolean] = new SqlExpressionTest[String, Boolean] {
      override protected def computeExpectedValue(input: String): Boolean = Option(input).isDefined && !StringUtils.isBlank(input)
    }

    val inputSamples: Seq[String] = null :: "" :: "  " :: "hello" :: "  world" :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[Substring].getSimpleName} function" in {

    val (pos, len): (Int, Int) = (1, 2)
    val expression = s"${FunctionName.Substring}(${SqlExpressionTest.firstColumnName}, $pos, $len)"
    val functionTest: SqlExpressionTest[String, String] = new SqlExpressionTest[String, String] {
      override protected def computeExpectedValue(input: String): String = input.substring(pos - 1, len - pos + 1)
    }

    val inputSamples: Seq[String] = "hello" :: "world" :: Nil
    functionTest.test(expression, inputSamples)
  }

  it should s"parse a ${classOf[ToDateOrTimestamp].getSimpleName} function" in {

    val (dtPattern, tsPattern) = ("yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")
    val (dtFormatter, tsFormatter) = (DateTimeFormatter.ofPattern(dtPattern), DateTimeFormatter.ofPattern(tsPattern))
    val dtGenerator: Int => String = i => LocalDate.now().minusDays(i).format(dtFormatter)
    val tsGenerator: Int => String = i => LocalDateTime.now().minusMinutes(i).format(tsFormatter)
    val testSet: Map[String, (String, Int => String, String => util.Date, Row => util.Date)] = Map(
      FunctionName.ToDate -> (dtPattern, dtGenerator, s => Date.valueOf(LocalDate.parse(s, dtFormatter)), r => r.getDate(0)),
      FunctionName.ToTimestamp -> (tsPattern, tsGenerator, s => Timestamp.valueOf(LocalDateTime.parse(s, tsFormatter)), r => r.getTimestamp(0))
    )

    testSet.foreach {
      case (key, (pattern, sampleGeneratorF, expectedF, actualValueF)) =>
        val expression = s"$key(${SqlExpressionTest.firstColumnName}, '$pattern')"
        val functionTest: SqlExpressionTest[String, _ <: util.Date] = new SqlExpressionTest[String, util.Date] {
          override protected def computeExpectedValue(input: String): util.Date = expectedF(input)
          override protected def getActualValue(r: Row): util.Date = actualValueF(r)
        }

        val inputSamples: Seq[String] = sampleGeneratorF(5) :: sampleGeneratorF(0) :: Nil
        functionTest.test(expression, inputSamples)
    }
  }
}
