package it.luca.aurora.core

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

package object implicits {

  implicit def toDataFrameWrapper(dataFrame: DataFrame): DataFrameWrapper = new DataFrameWrapper(dataFrame)

  implicit def toFileSystemWrapper(fs: FileSystem): FileSystemWrapper = new FileSystemWrapper(fs)

  implicit def toSparkSessionWrapper(sparkSession: SparkSession): SparkSessionWrapper = new SparkSessionWrapper(sparkSession)
}
