package it.luca.aurora.core

import org.apache.spark.sql.SparkSession

trait SparkBasicTest
  extends BasicTest {

  protected final val sparkSession: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName(classOf[SparkBasicTest].getSimpleName)
    .getOrCreate()

}
