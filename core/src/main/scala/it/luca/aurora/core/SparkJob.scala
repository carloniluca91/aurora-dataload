package it.luca.aurora.core

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, SQLException}

abstract class SparkJob(protected val sparkSession: SparkSession,
                        protected val impalaJDBCConnection: Connection)
  extends Logging {

  /**
   * Saves a [[DataFrame]] to a Hive table and issues an ImpalaQL statement to make data available to Impala
   * @param dataFrame [[DataFrame]] to be saved
   * @param fqTableName fully qualified (i.e. db.table) name of target table
   * @param partitionColumn partition column
   */

  protected def saveAsOrInsertInto(dataFrame: DataFrame,
                                   fqTableName: String,
                                   partitionColumn: String): Unit = {

    val saveMode = SaveMode.Append
    val dataFrameClass = classOf[DataFrame].getSimpleName
    log.info(s"Saving given $dataFrameClass to target table $fqTableName. Schema\n\n${dataFrame.schema.treeString}")
    val tableExists: Boolean = sparkSession.catalog.tableExists(fqTableName)
    if (tableExists) {

      log.info(s"Target table $fqTableName already exists. Matching given $dataFrameClass to it and saving using .insertInto")
      val targetTableColumns: Seq[String] = sparkSession.table(fqTableName).columns
      dataFrame.selectExpr(targetTableColumns: _*)
        .write.mode(saveMode)
        .insertInto(fqTableName)
    } else {

      log.warn(s"Target table $fqTableName does not exist. Creating it now using .saveAsTable")
      dataFrame.write.mode(saveMode)
        .format("parquet")
        .partitionBy(partitionColumn)
        .saveAsTable(fqTableName)
    }

    executeImpalaStatement(fqTableName, tableExists)
  }

  /**
   * Execute an INVALIDATE METADATA or REFRESH ImpalaQLStatement depending on table existence
   * @param fqTableName fully qualified table name
   * @param tableExists whether given table already exists or not
   * @throws java.sql.SQLException if statement execution fails
   */

  @throws[SQLException]
  protected def executeImpalaStatement(fqTableName: String, tableExists: Boolean): Unit = {

    val impalaQLStatement: String = if (!tableExists) {
      s"INVALIDATE METADATA $fqTableName"
    } else s"REFRESH $fqTableName"

    log.info(s"Successfully saved data into $fqTableName. Issuing following ImpalaQL statement: $impalaQLStatement")
    impalaJDBCConnection.createStatement().execute(impalaQLStatement)
    log.info(s"Successfully issued following ImpalaQL statement: $impalaQLStatement")
  }
}
