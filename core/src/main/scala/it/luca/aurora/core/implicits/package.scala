package it.luca.aurora.core

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

package object implicits {

  implicit def toDataFrameWrapper(dataFrame: DataFrame): DataFrameWrapper = new DataFrameWrapper(dataFrame)

  implicit def toSparkContextWrapper(sc: SparkContext): SparkContextWrapper = new SparkContextWrapper(sc)
}
