package it.luca.aurora.configuration.metadata.load

import it.luca.aurora.configuration.ObjectDeserializer
import it.luca.aurora.configuration.metadata.JsonField
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class PartitionInfoTest
  extends AnyFlatSpec
    with should.Matchers {

  private  val columnName = "columnName"

  s"A ${classOf[PartitionInfo].getSimpleName}" should
    s"be deserialized as an instance of ${classOf[ColumnExpressionInfo].getSimpleName} " +
      s"when ${JsonField.TYPE} = ${PartitionInfo.COLUMN_EXPRESSION}" in {

    val columnExpression = "columnExpression"
    val json =
      s"""
         |{
         |   "${JsonField.TYPE}": "${PartitionInfo.COLUMN_EXPRESSION}",
         |   "${JsonField.COLUMN_NAME}": "$columnName",
         |   "${JsonField.COLUMN_EXPRESSION}": "$columnExpression"
         |}""".stripMargin

    val partitionInfo = ObjectDeserializer.deserializeString(json, classOf[PartitionInfo])
    partitionInfo.isInstanceOf[ColumnExpressionInfo] shouldBe true
    val columnExpressionInfo = partitionInfo.asInstanceOf[ColumnExpressionInfo]
    columnExpressionInfo.getType shouldEqual PartitionInfo.COLUMN_EXPRESSION
    columnExpressionInfo.getColumnName shouldEqual columnName
    columnExpressionInfo.getColumnExpression shouldEqual columnExpression
  }

  it should s"be deserialized as an instance of ${classOf[FileNameRegexInfo].getSimpleName} " +
    s"when ${JsonField.TYPE} = ${PartitionInfo.FILE_NAME_REGEX}" in {

    val (regexGroup, inputPattern, outputPattern) = (1, "yyyyMMdd", "yyyy-MM-dd")
    val fileNameRegexConfiguration =
      s"""
         |{
         |    "${JsonField.REGEX_GROUP}": $regexGroup,
         |    "${JsonField.INPUT_PATTERN}": "$inputPattern",
         |    "${JsonField.OUTPUT_PATTERN}": "$outputPattern"
         |}""".stripMargin

    val json =
      s"""
         |{
         |    "${JsonField.TYPE}": "${PartitionInfo.FILE_NAME_REGEX}",
         |    "${JsonField.COLUMN_NAME}": "$columnName",
         |    "${JsonField.CONFIGURATION}": $fileNameRegexConfiguration
         |}""".stripMargin

    val partitionInfo = ObjectDeserializer.deserializeString(json, classOf[PartitionInfo])
    partitionInfo.isInstanceOf[FileNameRegexInfo] shouldBe true
    val fileNameRegexInfo = partitionInfo.asInstanceOf[FileNameRegexInfo]
    fileNameRegexInfo.getType shouldEqual PartitionInfo.FILE_NAME_REGEX
    val configuration = fileNameRegexInfo.getConfiguration
    configuration.getRegexGroup shouldEqual regexGroup
    configuration.getInputPattern shouldEqual inputPattern
    configuration.getOutputPattern shouldEqual outputPattern
  }
}
