package it.luca.aurora.core.implicits

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}

class SparkSessionWrapper(protected val sparkSession: SparkSession) {

  /**
   * Get underlying instance of [[FileSystem]]
   * @return instance of [[FileSystem]]
   */

  def getFileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  /**
   * Get the location of a table
   * @param fqTableName fully qualified (i.e. db.table) table name
   * @return HDFS location of given table
   */

  def getTableLocation(fqTableName: String): String = {

    sparkSession.sql(s"DESCRIBE FORMATTED $fqTableName")
      .filter(lower(col("col_name")) === "location")
      .select(col("data_type"))
      .collect.head
      .getAs[String](0)
  }
}
