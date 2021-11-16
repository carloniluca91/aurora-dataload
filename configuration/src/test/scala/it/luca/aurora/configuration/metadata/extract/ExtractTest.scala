package it.luca.aurora.configuration.metadata.extract

import it.luca.aurora.configuration.metadata.DeserializationTest

class ExtractTest
  extends DeserializationTest {

  private val landingPath = "landingPath"
  private val fileNameRegex = "fileNameRegex"

  s"An instance of ${nameOf[Extract]}" should
    s"be deserialized as an instance of ${nameOf[AvroExtract]} when ${Extract.Type} = ${Extract.Avro}" in {

    val json =
      s"""
         |{
         |   "${Extract.Type}": "${Extract.Avro}",
         |   "${Extract.LandingPath}": "$landingPath",
         |   "${Extract.FileNameRegex}": "$fileNameRegex"
         |}""".stripMargin

    val extract: Extract = mapper.readValue(json, classOf[Extract])
    extract.extractType shouldBe Extract.Avro
    extract.landingPath shouldBe landingPath
    extract.fileNameRegex shouldBe fileNameRegex
    extract.isInstanceOf[AvroExtract] shouldBe true
  }

  it should
    s"be deserialized as an instance of ${nameOf[CsvExtract]} when ${Extract.Type} = ${Extract.Csv} and no options are provided" in {

    val json =
      s"""
         |{
         |   "${Extract.Type}": "${Extract.Csv}",
         |   "${Extract.LandingPath}": "$landingPath",
         |   "${Extract.FileNameRegex}": "$fileNameRegex"
         |}""".stripMargin

    val extract: Extract = mapper.readValue(json, classOf[Extract])
    extract.extractType shouldBe Extract.Csv
    extract.landingPath shouldBe landingPath
    extract.fileNameRegex shouldBe fileNameRegex
    extract.isInstanceOf[CsvExtract] shouldBe true

    val csvExtract: CsvExtract = extract.asInstanceOf[CsvExtract]
    csvExtract.options shouldBe None
  }

  it should
    s"be deserialized as an instance of ${nameOf[CsvExtract]} when ${Extract.Type} = ${Extract.Csv} and some options are provided" in {

    val (key, value) = ("sep", "|")
    val json =
      s"""
         |{
         |   "${Extract.Type}": "${Extract.Csv}",
         |   "${Extract.LandingPath}": "$landingPath",
         |   "${Extract.FileNameRegex}": "$fileNameRegex",
         |   "options": {
         |      "$key": "$value"
         |   }
         |}""".stripMargin

    val extract: Extract = mapper.readValue(json, classOf[Extract])
    extract.extractType shouldBe Extract.Csv
    extract.landingPath shouldBe landingPath
    extract.fileNameRegex shouldBe fileNameRegex
    extract.isInstanceOf[CsvExtract] shouldBe true

    val csvExtract: CsvExtract = extract.asInstanceOf[CsvExtract]
    csvExtract.options shouldBe Some(_: Map[String, String])

    val options: Map[String, String] = csvExtract.options.get
    options.size shouldBe 1
    options(key) shouldBe value
  }
}

