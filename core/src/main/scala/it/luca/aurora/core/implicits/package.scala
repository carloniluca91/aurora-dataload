package it.luca.aurora.core

import org.apache.spark.sql.DataFrame

package object implicits {

  implicit def toExtendedDataFrame(dataFrame: DataFrame): ExtendedDataFrame = new ExtendedDataFrame(dataFrame)
}
