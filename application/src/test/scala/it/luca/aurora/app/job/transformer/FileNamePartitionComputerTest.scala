package it.luca.aurora.app.job.transformer

import it.luca.aurora.configuration.metadata.transform.FileNamePartitioning
import it.luca.aurora.core.BasicTest
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class FileNamePartitionComputerTest
  extends BasicTest {

  s"A ${nameOf[FileNamePartitionComputer]}" should "correctly retrieve partition column value" in {

    val now: LocalDate = LocalDate.now()
    val (inputPattern, outputPattern) = ("yyyyMMdd", "yyyy-MM-dd")
    val fileName: String = s"fileName${now.format(DateTimeFormatter.ofPattern(inputPattern))}.csv"
    val regex: String = "^fileName(\\d{8})\\.csv$"
    val path: Path = new Path(s"/var/data/$fileName")

    val partitioning: FileNamePartitioning = FileNamePartitioning("IGNORE", "IGNORE", 1, inputPattern, outputPattern)
    val expectedValue: String = now.format(DateTimeFormatter.ofPattern(outputPattern))
    new FileNamePartitionComputer(path, regex.r, partitioning).getPartitionColumn shouldBe lit(expectedValue)
  }
}
