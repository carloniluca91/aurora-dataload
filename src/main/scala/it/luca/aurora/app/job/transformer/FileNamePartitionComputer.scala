package it.luca.aurora.app.job.transformer

import it.luca.aurora.configuration.metadata.transform.FileNamePartitioning
import it.luca.aurora.core.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

/**
 * Partition column computer based on input data's file name
 * @param filePath [[Path]] of input data file
 * @param fileNameRegex regex for extracting date information from input data's file name
 * @param partitioning instance of [[FileNamePartitioning]]
 */

class FileNamePartitionComputer(protected val filePath: Path,
                                protected val fileNameRegex: Regex,
                                override protected val partitioning: FileNamePartitioning)
  extends PartitionColumnComputer[FileNamePartitioning](partitioning)
    with Logging {

  override def getPartitionColumn: Column = {

    val fileName: String = filePath.getName
    val dateFromFileName: String = fileNameRegex.findFirstMatchIn(fileName) match {
      case Some(value) => LocalDate
        .parse(value.group(partitioning.regexGroup), DateTimeFormatter.ofPattern(partitioning.inputPattern))
        .format(DateTimeFormatter.ofPattern(partitioning.outputPattern))
      case None => throw new IllegalArgumentException(s"Given file name ($fileName) does not macth given regex ($fileNameRegex)")
    }

    log.info(s"Value for partition column ${partitioning.columnName} will be $dateFromFileName")
    lit(dateFromFileName)
  }
}