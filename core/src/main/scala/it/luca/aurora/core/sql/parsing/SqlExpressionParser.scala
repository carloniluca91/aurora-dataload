package it.luca.aurora.core.sql.parsing

import it.luca.aurora.core.logging.Logging
import it.luca.aurora.core.sql.functions.{FunctionName, MatchesDateOrTimestampFormat, MultipleColumnFunction, SingleColumnFunction, SqlFunction, ToDateOrTimestamp}
import net.sf.jsqlparser.expression.operators.relational.{ExpressionList, InExpression, IsNullExpression}
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.{expression, schema}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}

import scala.collection.JavaConversions.asScalaBuffer

object SqlExpressionParser
  extends Logging {

  /**
   * Converts a string representing a SQL expression to a [[Column]]
   *
   * @param input input string
   * @return instance of [[Column]]
   */

  @throws(classOf[UnidentifiedExpressionException])
  def parse(input: String): Column = {

    val outputColumn: Column = CCJSqlParserUtil.parseCondExpression(input, false) match {

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

  def parse(expression: Expression): Column = parse(expression.toString)

  /**
   * Converts a subclass of [[BinaryExpression]] to a [[Column]]
   *
   * @param expression input expression
   * @return instance of [[Column]]
   */

  def parseSqlBinaryExpression(expression: BinaryExpression): Column = {

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
   *
   * @param expression input expression
   * @return instance of [[Column]]
   */

  def parseIsNullExpression(expression: IsNullExpression): Column = {

    val leftColumn = parse(expression.getLeftExpression)
    log.debug(s"Parsed left expression for ${classOf[IsNullExpression].getSimpleName}")
    if (expression.isNot) leftColumn.isNotNull else leftColumn.isNull
  }

  /**
   * Converts an instance of [[InExpression]] to a [[Column]]
   *
   * @param expression input expression
   * @return instance of [[Column]]
   */

  def parseInExpression(expression: InExpression): Column = {

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

  def parseCaseExpression(expression: CaseExpression): Column = {

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

  def parseSqlFunction(function: expression.Function): Column = {

    // Standard Sql functions
    val sqlFunction: SqlFunction = function.getName.toLowerCase match {

      //case "concat" => Concat(function)
      //case "concat_ws" => ConcatWs(function)
      //case "lpad" | "rpad" => LeftOrRightPad(function)
      //case "substring" => Substring(function)
      case FunctionName.MatchesDateFormat | FunctionName.MatchesTimestampFormat => MatchesDateOrTimestampFormat(function)
      case FunctionName.ToDate | FunctionName.ToTimestamp => ToDateOrTimestamp(function)
    }

    sqlFunction match {
      case s: SingleColumnFunction =>

        val inputColumn = parse(function.getParameters.getExpressions.get(0))
        s.getColumn(inputColumn)

      case m: MultipleColumnFunction =>

        // Input parameters corresponding to input columns
        val inputColumnExpressions: java.util.List[Expression] = m match {

          // ConcatWs: exclude first parameter (which is separator)
          // case _: ConcatWs => function.getParameters.getExpressions.tail
          case _ => function.getParameters.getExpressions
        }

        val inputColumns: Seq[Column] = inputColumnExpressions.map(parse)
        log.info(s"Parsed all of ${inputColumnExpressions.size()} input column(s) for ${m.getClass.getSimpleName} function")
        m.getColumn(inputColumns: _*)
    }
  }
}
