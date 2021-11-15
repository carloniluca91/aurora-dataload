package it.luca.aurora.app.job.transformer

import it.luca.aurora.configuration.metadata.transform.FileNamePartitioning
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

/**
 * Partition column computer based on input data's file name
 */

object FileNamePartitionComputer
  extends PartitionColumnComputer[(Path, Regex, FileNamePartitioning)] {

  override def getPartitionColumn(input: (Path, Regex, FileNamePartitioning)): Column = {

    val (path, regex, partitioning): (Path, Regex, FileNamePartitioning) = input
    val fileName: String = path.getName
    val dateFromFileName: String = regex.findFirstMatchIn(fileName) match {
      case Some(value) => LocalDate
        .parse(value.group(partitioning.regexGroup), DateTimeFormatter.ofPattern(partitioning.inputPattern))
        .format(DateTimeFormatter.ofPattern(partitioning.outputPattern))
      case None => throw new IllegalArgumentException(s"Given file name ($fileName) does not macth given regex ($regex)")
    }

    log.info(s"Value for partition column ${partitioning.columnName} will be $dateFromFileName")
    lit(dateFromFileName)
  }
}