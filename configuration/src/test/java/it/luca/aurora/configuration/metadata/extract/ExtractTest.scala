package it.luca.aurora.configuration.metadata.extract

import it.luca.aurora.configuration.ObjectDeserializer
import it.luca.aurora.configuration.metadata.JsonField
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util.Optional

class ExtractTest
  extends AnyFlatSpec
    with should.Matchers {

  s"An ${classOf[Extract].getSimpleName}" should
    s"be deserialized as an instance of ${classOf[CsvExtract].getSimpleName} when ${JsonField.TYPE} = ${Extract.CSV}" in {

    val fileNameRegex = "fileNameRegex"
    val json =
      s"""
        |{
        |   "${JsonField.TYPE}": "${Extract.CSV}",
        |   "${JsonField.FILE_NAME_REGEX}": "fileNameRegex",
        |   "${JsonField.CONFIGURATION}": {
        |     "${JsonField.OPTIONS}": {
        |       "sep": ";"
        |     }
        |   }
        |}""".stripMargin

    val extract: Extract = ObjectDeserializer.deserializeString(json, classOf[Extract])
    extract.isInstanceOf[CsvExtract] shouldBe true
    val csvExtract: CsvExtract = extract.asInstanceOf[CsvExtract]
    csvExtract.getFileNameRegex shouldEqual fileNameRegex
    Optional.ofNullable(csvExtract.getConfiguration).isPresent shouldBe true
    Optional.ofNullable(csvExtract.getConfiguration.getOptions).isPresent shouldBe true
    csvExtract.getConfiguration.getOptions.isEmpty shouldBe false
  }
}
