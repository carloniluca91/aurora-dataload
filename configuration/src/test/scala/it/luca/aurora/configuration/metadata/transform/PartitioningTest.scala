package it.luca.aurora.configuration.metadata.transform

import it.luca.aurora.configuration.metadata.DeserializationTest
import it.luca.aurora.configuration.metadata.transform.ColumnPartitioning._
import it.luca.aurora.configuration.metadata.transform.FileNameRegexPartitioning._
import it.luca.aurora.configuration.metadata.transform.Partitioning._

class PartitioningTest
  extends DeserializationTest {

  private val columnName: String = "columnName"

  s"A ${nameOf[Partitioning]}" should
    s"be deserialized as an instance of ${nameOf[FileNameRegexPartitioning]} when $Type = $FileNameRegex" in {

    val (regexGroup, inputPattern, outputPattern) = (1, "yyyyMMdd", "yyyy-MM-dd")
    val json =
      s"""
        |{
        |   "$Type": "${Partitioning.FileNameRegex}",
        |   "$ColumnName": "$columnName",
        |   "$RegexGroup": "$regexGroup",
        |   "$InputPattern": "$inputPattern",
        |   "$OutputPattern": "$outputPattern"
        |}""".stripMargin

    val partitioning: Partitioning = mapper.readValue(json, classOf[Partitioning])
    partitioning.partitioningType shouldBe FileNameRegex
    partitioning.columnName shouldBe columnName
    partitioning.isInstanceOf[FileNameRegexPartitioning] shouldBe true

    val fileNameRegexPartitioning: FileNameRegexPartitioning = partitioning.asInstanceOf[FileNameRegexPartitioning]
    fileNameRegexPartitioning.regexGroup shouldBe regexGroup
    fileNameRegexPartitioning.inputPattern shouldBe inputPattern
    fileNameRegexPartitioning.outputPattern shouldBe outputPattern
  }

  it should s"be deserialized as an instance of ${nameOf[ColumnPartitioning]} when $Type = $Column" in {

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
    partitioning.isInstanceOf[ColumnPartitioning] shouldBe true

    val columnPartitioning: ColumnPartitioning = partitioning.asInstanceOf[ColumnPartitioning]
    columnPartitioning.columnExpression shouldBe columnExpression
  }
}
