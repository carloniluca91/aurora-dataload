package it.luca.aurora.core.sql.parsing

import it.luca.aurora.core.Logging
import it.luca.aurora.core.sql.functions._
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.relational.{ExpressionList, InExpression, IsNullExpression}
import net.sf.jsqlparser.parser.CCJSqlParserUtil.parseExpression
import net.sf.jsqlparser.{expression, schema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}

import scala.collection.JavaConversions._
import scala.util.matching.Regex

object SqlExpressionParser
  extends Logging {

  /**
   * Converts a string representing a SQL expression to a [[Column]]
   * @param input input SQL string
   * @throws it.luca.aurora.core.sql.parsing.UnidentifiedExpressionException if parsing fails
   * @return instance of [[Column]]
   */

  @throws[UnidentifiedExpressionException]
  def parse(input: String): Column = {

    val aliasExpressionRegex: Regex = "^(.+) as (\\w+)$".r
    val eitherInputStrOrColumn: Either[String, Column] = aliasExpressionRegex
      .findFirstMatchIn(input) match {
      case Some(regexMatch) => Right(parse(regexMatch.group(1)).as(regexMatch.group(2)))
      case None => Left(input)
    }

    eitherInputStrOrColumn match {
      case Right(column) => column
      case Left(str) =>

        val outputColumn: Column = parseExpression(str, false) match {

          case c: schema.Column => col(c.getColumnName)
          case s: StringValue => lit(s.getValue)
          case l: LongValue => lit(l.getValue.toInt)
          case parenthesis: Parenthesis => parse(parenthesis.getExpression)
          case caseWhen: CaseExpression => parseCaseExpression(caseWhen)
          case binaryExpression: BinaryExpression => parseSqlBinaryExpression(binaryExpression)
          case isNullExpression: IsNullExpression => parseIsNullExpression(isNullExpression)
          case inExpression: InExpression => parseInExpression(inExpression)
          case function: expression.Function => parseSqlFunction(function)
          case _ => throw new UnidentifiedExpressionException(input)
        }

        log.info(s"Successfully parsed input string $input as an instance of ${classOf[Column].getSimpleName}")
        outputColumn
    }
  }

  protected def parse(expression: Expression): Column = parse(expression.toString)

  /**
   * Converts a subclass of [[BinaryExpression]] to a [[Column]]
   * @param expression input expression
   * @return instance of [[Column]]
   */

  protected def parseSqlBinaryExpression(expression: BinaryExpression): Column = {

    // BinaryExpressions (<, <=, =, <>, >, >=, AND, OR)
    val leftColumn = parse(expression.getLeftExpression)
    val rightColumn = parse(expression.getRightExpression)
    log.debug(s"Parsed both left and right expression of ${classOf[BinaryExpression].getSimpleName} $expression")
    val combinator: (Column, Column) => Column = expression.getStringExpression.toLowerCase match {
      case "<" => _ < _
      case "<=" => _ <= _
      case "=" => _ === _
      case "<>" => _ =!= _
      case ">=" => _ >= _
      case ">" => _ > _
      case "and" => _ && _
      case "or" => _ || _
    }

    combinator(leftColumn, rightColumn)
  }

  /**
   * Converts an instance of [[IsNullExpression]] to a [[Column]]
   * @param expression input expression
   * @return instance of [[Column]]
   */

  protected def parseIsNullExpression(expression: IsNullExpression): Column = {

    val leftColumn = parse(expression.getLeftExpression)
    log.debug(s"Parsed left expression for ${classOf[IsNullExpression].getSimpleName}")
    if (expression.isNot) leftColumn.isNotNull else leftColumn.isNull
  }

  /**
   * Converts an instance of [[InExpression]] to a [[Column]]
   * @param expression input expression
   * @return instance of [[Column]]
   */

  protected def parseInExpression(expression: InExpression): Column = {

    val leftColumn = parse(expression.getLeftExpression)
    val inValuesColumns: Seq[Column] = expression
      .getRightItemsList(classOf[ExpressionList])
      .getExpressions
      .map(parse)

    log.debug(s"Parsed both left and all of ${inValuesColumns.size} right expression(s) of ${classOf[InExpression].getSimpleName}")
    val isInColumn = leftColumn.isin(inValuesColumns: _*)
    if (expression.isNot) !isInColumn else isInColumn
  }

  /**
   * Converts an instance of [[CaseExpression]] to a [[Column]]
   *
   * @param expression input expression
   * @return instance of [[Column]]
   */

  protected def parseCaseExpression(expression: CaseExpression): Column = {

    val whenCases: Seq[(Column, Column)] = expression.getWhenClauses.map(x => (parse(x.getWhenExpression), parse(x.getThenExpression)))
    val elseValue: Column = parse(expression.getElseExpression)
    log.debug(s"Parsed both all of ${expression.getWhenClauses.size()} ${classOf[WhenClause].getSimpleName}(s) and ElseExpression")
    val firstCase: Column = when(whenCases.head._1, whenCases.head._2)
    whenCases.tail
      .foldLeft(firstCase)((col, tuple2) => col.when(tuple2._1, tuple2._2))
      .otherwise(elseValue)
  }

  /**
   * Converts an instance of [[Function]] to a [[Column]]
   *
   * @param function input expression
   * @return instance of [[Column]]
   */

  protected def parseSqlFunction(function: expression.Function): Column = {

    // Standard SQL functions
    val sqlFunction: SqlFunction = function.getName.toLowerCase match {

      case FunctionName.Concat => Concat(function)
      case FunctionName.ConcatWs => ConcatWs(function)
      case FunctionName.DateFormat => DateFormat(function)
      case FunctionName.LeftPad | FunctionName.RightPad => LeftOrRightPad(function)
      case FunctionName.MatchesDateFormat | FunctionName.MatchesTimestampFormat => MatchesDateOrTimestampFormat(function)
      case FunctionName.Substring => Substring(function)
      case FunctionName.ToDate | FunctionName.ToTimestamp => ToDateOrTimestamp(function)
      case FunctionName.LeftTrim | FunctionName.RightTrim | FunctionName.Trim => LeftOrRightOrBothTrim(function)
      case _ => throw new UnmatchedSQLFunction(function)
    }

    sqlFunction match {
      case s: SingleColumnFunction =>

        val inputColumn = parse(function.getParameters.getExpressions.get(0))
        s.transform(inputColumn)

      case m: MultipleColumnFunction =>

        // Input parameters corresponding to input columns
        val inputColumnExpressions: Seq[Expression] = m match {

          // ConcatWs: exclude first parameter (which is separator)
          case _: ConcatWs => function.getParameters.getExpressions.tail
          case _ => function.getParameters.getExpressions
        }

        val inputColumns: Seq[Column] = inputColumnExpressions.map(parse)
        log.info(s"Parsed all of ${inputColumnExpressions.size} input column(s) for ${m.getClass.getSimpleName} function")
        m.transform(inputColumns: _*)
    }
  }
}
