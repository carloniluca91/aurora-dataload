package it.luca.aurora.app.job.transformer

import it.luca.aurora.app.job.transformer.Transformer._
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.transform.{ColumnExpressionPartitioning, FileNamePartitioning, Transform}
import it.luca.aurora.core.Logging
import it.luca.aurora.core.implicits._
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Input data transformer
 * @param extract dataSource's extract specifications
 * @param transform dataSource's transform specifications
 */

class Transformer(protected val extract: Extract,
                  protected val transform: Transform)
  extends Logging {

  /**
   * Transform input dataFrame
   * @param dataFrame input dataFrame
   * @param filePath [[Path]] of input dataFrame
   * @return tuple with both trusted layer dataFrame and error dataFrame
   */

  def transform(dataFrame: DataFrame, filePath: Path): (DataFrame, DataFrame) = {

    val filterStatementsAndCols: Seq[(String, Column)] = transform.filters.map { x => (x, SqlExpressionParser.parse(x)) }
    log.info(s"Successfully parsed all of ${filterStatementsAndCols.size} filter(s)")
    val overallFilterCol: Column = filterStatementsAndCols.map { case (_, column) => column }.reduce(_ && _)
    val filterFailureReportCols: Seq[Column] = filterStatementsAndCols.map {
      case (string, column) => when(!column, string).otherwise(NoFailedCheckString) }

    // Partitioning
    val partitionColumnName: String = transform.partitioning.columnName
    val partitionCol: Column = transform.partitioning match {
      case c: ColumnExpressionPartitioning => ColumnExpressionPartitionComputer.getPartitionColumn(c)
      case f: FileNamePartitioning => FileNamePartitionComputer.getPartitionColumn((filePath, extract.fileNameRegex.r, f))
    }

    // Valid records
    val trustedDfColumns: Seq[Column] = transform.transformations.map { SqlExpressionParser.parse }
    log.info(s"Successfully parsed all of ${trustedDfColumns.size} trasformation(s)")
    val nonFinalTrustedDf: DataFrame = dataFrame.filter(overallFilterCol)
      .select(trustedDfColumns: _*)
      .withInputFilePathCol(InputFilePath, filePath)
      .withTechnicalColumns()
      .withColumn(partitionColumnName, partitionCol)

    // Optionally remove duplicates and drop columns
    log.info(s"Successfully applied all of ${trustedDfColumns.size} transformation(s)")
    val finalTrustedDf: DataFrame = maybeDropDuplicatesAndColumns(nonFinalTrustedDf)

    // Invalid records (i.e. that do not satisfy all of dataSource filters)
    val errorDf: DataFrame = dataFrame.filter(!overallFilterCol)
      .withColumn(FailedChecksNumber, size(array_remove(array(filterFailureReportCols: _*), NoFailedCheckString)))
      .withColumn(FailedChecks, array(filterFailureReportCols: _*))
      .withInputFilePathCol(InputFilePath, filePath)
      .withTechnicalColumns()
      .withColumn(partitionColumnName, partitionCol)

    (finalTrustedDf, errorDf)
  }

  /**
   * Remove duplicates and drop columns from a given [[DataFrame]]
   * @param dataFrame input dataFrame
   * @return input dataFrame with
   */

  protected final def maybeDropDuplicatesAndColumns(dataFrame: DataFrame): DataFrame = {

    val dataFrameClassName: String = classOf[DataFrame].getSimpleName
    val describe: Seq[String] => String = seq => seq.map(x => s"  $x").mkString("\n").concat("\n")
    val dataFrameMaybeWithDroppedDuplicates: DataFrame = transform.dropDuplicates match {
      case Some(value) =>
        log.info(s"Dropping duplicates from given $dataFrameClassName computed along columns\n\n${describe(value)}")
        dataFrame.dropDuplicates(value)
      case None =>
        log.info(s"No duplicates will be removed from given $dataFrameClassName")
        dataFrame
    }

    transform.dropColumns match {
      case Some(value) =>
        log.info(s"Dropping following columns from given $dataFrameClassName\n\n${describe(value)}")
        dataFrameMaybeWithDroppedDuplicates.drop(value: _*)
      case None =>
        log.info(s"No columns to remove from given $dataFrameClassName")
        dataFrameMaybeWithDroppedDuplicates
    }
  }
}

object Transformer {

  val InputFilePath: String = "input_file_path"
  val FailedChecksNumber = "failed_checks_number"
  val FailedChecks: String = "failed_checks"
  val NoFailedCheckString: String = "NoFailedCheck"
}
