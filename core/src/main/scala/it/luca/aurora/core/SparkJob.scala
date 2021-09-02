package it.luca.aurora.core

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import java.sql.{Connection, SQLException}

abstract class SparkJob(protected val sparkSession: SparkSession,
                        protected val impalaJDBCConnection: Connection)
  extends Logging {

  /**
   * Saves a [[DataFrame]] to a Hive table and issues an ImpalaQL statement to make data available to Impala
   * @param dataFrame [[DataFrame]] to be saved
   * @param fqTableName fully qualified (i.e. db.table) name of target table
   * @param saveMode [[SaveMode]] to be used
   * @param partitionByColumnOpt optional column to be used for partitioning
   * @throws java.sql.SQLException if ImpalaQL statement fails
   */

  @throws[SQLException]
  private def saveAsOrInsertInto(dataFrame: DataFrame,
                                 fqTableName: String,
                                 saveMode: SaveMode,
                                 partitionByColumnOpt: Option[String]): Unit = {

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

      // Connect to Impala and execute statement
      val impalaQLStatement: String = if (!tableExists || saveMode == SaveMode.Overwrite) {
        s"INVALIDATE METADATA $fqTableName"
      } else s"REFRESH $fqTableName"

      log.info(s"Successfully saved data into $fqTableName. Issuing following ImpalaQL statement: $impalaQLStatement")
      impalaJDBCConnection.createStatement().execute(impalaQLStatement)
      log.info(s"Successfully issued following ImpalaQL statement: $impalaQLStatement")
    }
  }

  /**
   * Saves a [[DataFrame]] to a Hive table with savemode "append" and issues an ImpalaQL statement to make data available to Impala
   * @param dataFrame [[DataFrame]] to be saved
   * @param fqTableName fully qualified (i.e. db.table) name of target table
   * @param partitionColumn name of partition column
   */

  protected def saveAsOrInsertIntoInAppend(dataFrame: DataFrame, fqTableName: String, partitionColumn: String): Unit =
    saveAsOrInsertInto(dataFrame, fqTableName, SaveMode.Append, Some(partitionColumn))
}
