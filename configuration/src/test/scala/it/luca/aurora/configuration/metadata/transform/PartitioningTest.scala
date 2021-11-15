package it.luca.aurora.configuration.metadata.transform

import it.luca.aurora.configuration.metadata.DeserializationTest
import it.luca.aurora.configuration.metadata.transform.ColumnExpressionPartitioning._
import it.luca.aurora.configuration.metadata.transform.FileNamePartitioning._
import it.luca.aurora.configuration.metadata.transform.Partitioning._

class PartitioningTest
  extends DeserializationTest {

  private val columnName: String = "columnName"

  s"A ${nameOf[Partitioning]}" should
    s"be deserialized as an instance of ${nameOf[FileNamePartitioning]} when $Type = $FileName" in {

    val (regexGroup, inputPattern, outputPattern) = (1, "yyyyMMdd", "yyyy-MM-dd")
    val json =
      s"""
        |{
        |   "$Type": "${Partitioning.FileName}",
        |   "$ColumnName": "$columnName",
        |   "$RegexGroup": "$regexGroup",
        |   "$InputPattern": "$inputPattern",
        |   "$OutputPattern": "$outputPattern"
        |}""".stripMargin

    val partitioning: Partitioning = mapper.readValue(json, classOf[Partitioning])
    partitioning.partitioningType shouldBe FileName
    partitioning.columnName shouldBe columnName
    partitioning.isInstanceOf[FileNamePartitioning] shouldBe true

    val fileNameRegexPartitioning: FileNamePartitioning = partitioning.asInstanceOf[FileNamePartitioning]
    fileNameRegexPartitioning.regexGroup shouldBe regexGroup
    fileNameRegexPartitioning.inputPattern shouldBe inputPattern
    fileNameRegexPartitioning.outputPattern shouldBe outputPattern
  }

  it should s"be deserialized as an instance of ${nameOf[ColumnExpressionPartitioning]} when $Type = $Column" in {

    val columnExpression = "columnExpression"
    val json =
      s"""
         |{
         |   "$Type": "${Partitioning.Column}",
         |   "$ColumnName": "$columnName",
         |   "$ColumnExpression": "$columnExpression"
         |}""".stripMargin

    val partitioning: Partitioning = mapper.readValue(json, classOf[Partitioning])
    partitioning.partitioningType shouldBe Column
    partitioning.columnName shouldBe columnName
    partitioning.isInstanceOf[ColumnExpressionPartitioning] shouldBe true

    val columnPartitioning: ColumnExpressionPartitioning = partitioning.asInstanceOf[ColumnExpressionPartitioning]
    columnPartitioning.columnExpression shouldBe columnExpression
  }
}
