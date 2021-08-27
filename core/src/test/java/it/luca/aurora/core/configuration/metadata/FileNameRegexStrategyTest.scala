package it.luca.aurora.core.configuration.metadata

import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class FileNameRegexStrategyTest
  extends AnyFlatSpec
    with should.Matchers
    with MockFactory {

  s"A ${classOf[FileNameRegexStrategy].getSimpleName}" should "correclty extract date from file name" in {

    val originalDate = LocalDate.of(2021, 1, 1)
    val (inputPattern, outputPattern) = ("yyyyMMdd", "yyyy-MM-dd")
    val fileName = s"fileName_${originalDate.format(DateTimeFormatter.ofPattern(inputPattern))}.csv.gz"
    val path: Path = new Path(s"/path/to/$fileName")

    val regexStrategy = new FileNameRegexStrategy("id", "columnName", 1, inputPattern, outputPattern)
    val date = regexStrategy.getDateFromFileName("^fileName_(\\d{8})\\.csv\\.gz$", path)
    date shouldEqual originalDate.format(DateTimeFormatter.ofPattern(outputPattern))
  }
}
