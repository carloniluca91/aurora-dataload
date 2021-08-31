package it.luca.aurora.configuration.metadata

import it.luca.aurora.configuration.metadata.load.{FileNameRegexConfiguration, FileNameRegexException, FileNameRegexInfo}
import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class FileNameRegexInfoTest
  extends AnyFlatSpec
    with should.Matchers
    with MockFactory {

  private val (inputPattern, outputPattern) = ("yyyyMMdd", "yyyy-MM-dd")
  private val originalDate = LocalDate.of(2021, 1, 1)
  private val fileName = s"fileName_${originalDate.format(DateTimeFormatter.ofPattern(inputPattern))}.csv.gz"
  private val path: Path = new Path(s"/path/to/$fileName")
  private val fileNameRegexConfiguration = new FileNameRegexConfiguration(1, inputPattern, outputPattern)

  s"A ${classOf[FileNameRegexInfo].getSimpleName}" should "correclty extract date from file name" in {

    val regexStrategy = new FileNameRegexInfo("id", "columnName", fileNameRegexConfiguration)
    val date = regexStrategy.getDateFromFileName("^fileName_(\\d{8})\\.csv\\.gz$", path)
    date shouldEqual originalDate.format(DateTimeFormatter.ofPattern(outputPattern))
  }

  it should s"throw a ${classOf[FileNameRegexException].getSimpleName} if file name does not match with given regex" in {

    val regexStrategy = new FileNameRegexInfo("id", "columnName", fileNameRegexConfiguration)
    a [FileNameRegexException] should be thrownBy {
     regexStrategy.getDateFromFileName("^otherName_(\\d{8})\\.csv\\.gz$", path)
    }
  }
}
