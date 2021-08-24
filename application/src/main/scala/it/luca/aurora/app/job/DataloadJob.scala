package it.luca.aurora.app.job

import it.luca.aurora.core.implicits._
import it.luca.aurora.core.configuration.metadata.DataSourceMetadata
import it.luca.aurora.core.configuration.yaml.DataSource
import it.luca.aurora.core.logging.{DataloadJobRecord, Logging}
import it.luca.aurora.core.sql.parsing.SqlExpressionParser
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat_ws, when}

import java.sql.{Connection, SQLException}
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DataloadJob(protected val sparkSession: SparkSession,
                  protected val dataSource: DataSource,
                  protected val dataSourceMetadata: DataSourceMetadata,
                  protected val impalaJDBCConnection: Connection)
  extends Logging {

  def run(inputFile: FileStatus): DataloadJobRecord = {

    Try {

      val inputFilePath: String = inputFile.getPath.toString
      val inputDataFrame: DataFrame = sparkSession.read
        .schema(dataSourceMetadata.getInputSchemaAsStructType)
        .csv(inputFilePath)

      log.info(s"Successfully read input file $inputFilePath")
      val filterCols: Seq[(String, Column)] = dataSourceMetadata.getFilters
        .toSeq.map { x => (x, SqlExpressionParser.parse(x)) }

      val overallFilterCol: Column = filterCols.map{ _._2 }.reduce(_ && _)
      val dataSourceId: String = dataSourceMetadata.getId
      log.info(s"Successfully parsed all of ${dataSourceMetadata.getFilters.size()} filter(s) for dataSource $dataSourceId")

      // Invalid records (i.e. that do not satisfy all of dataSource filters)
      val filterFailureCols: Seq[Column] = filterCols.map { x => when(x._2, x._1) }
      val invalidRecordsDataFrame: DataFrame = inputDataFrame
        .filter(!overallFilterCol)
        .withColumn("failed_checks", concat_ws(", ", filterFailureCols: _*))
        .withTechnicalColumns()

      // Valid records
      val trustedDataFrameColumns: Seq[Column] = dataSourceMetadata.getTrasformations
        .toSeq.map { x => {
        val column: Column = SqlExpressionParser.parse(x.getExpression)
        if (x.isAliasPresent) column.as(x.getAlias) else column }
      }

      val numberOfTransformations: Int = dataSourceMetadata.getTrasformations.size()
      log.info(s"Successfully converted all of $numberOfTransformations trasformation(s) for dataSource $dataSourceId")
      val validRecordsDataFrame: DataFrame = inputDataFrame
        .filter(overallFilterCol)
        .select(trustedDataFrameColumns: _*)
        .withTechnicalColumns()

      log.info(s"Successfully added all of $numberOfTransformations for dataSource $dataSourceId")

    } match {
      case Success(_) => DataloadJobRecord(sparkSession.sparkContext, dataSource, inputFile, "x", None)
      case Failure(exception) => DataloadJobRecord(sparkSession.sparkContext, dataSource, inputFile, "x", Some(exception))
    }
  }

  @throws(classOf[SQLException])
  private def write(dataFrame: DataFrame, fqTableName: String, saveMode: SaveMode, partitionByColumnOpt: Option[String]): Unit = {

    val dfClass: String = classOf[DataFrame].getSimpleName
    val cachedDataFrame: DataFrame = dataFrame.cache()
    if (cachedDataFrame.isEmpty) {
      log.warn(s"Given $dfClass for target table $fqTableName is empty. Thus, no data will be written to it")
    } else {

      log.info(s"Saving given $dfClass to target table $fqTableName with saveMode $saveMode. Schema\n\n${cachedDataFrame.schema.treeString}")
      val tableExists: Boolean = sparkSession.catalog.tableExists(fqTableName)
      if (tableExists) {
        log.info(s"Target table $fqTableName already exists. Matching given $dfClass to it and saving using .insertInto")
        cachedDataFrame.select(sparkSession.table(fqTableName).columns.map(col): _*)
          .write.mode(saveMode)
          .insertInto(fqTableName)
      } else {
        log.warn(s"Target table $fqTableName does not exist now. Creating it now using .saveAsTable")
        val commonWriter: DataFrameWriter[Row] = cachedDataFrame.write.mode(saveMode).format("parquet")
        val maybePartitionedWriter: DataFrameWriter[Row] = partitionByColumnOpt match {
          case Some(x) => commonWriter.partitionBy(x)
          case None => commonWriter
        }

        maybePartitionedWriter.saveAsTable(fqTableName)
      }

      // Connect to Impala and trigger statement
      val impalaQLStatement: String = if (!tableExists || saveMode == SaveMode.Overwrite) {
        s"INVALIDATE METADATA $fqTableName"
      } else s"REFRESH $fqTableName"

      log.info(s"Successfully saved data into $fqTableName. Issuing following ImpalaQL statement: $impalaQLStatement")
      impalaJDBCConnection.createStatement().execute(impalaQLStatement)
      log.info(s"Successfully issued following ImpalaQL statement: $impalaQLStatement")
    }
  }
}
