package it.luca.aurora.core.sql.functions

import net.sf.jsqlparser.expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat

case class Concat(override protected val function: expression.Function)
  extends MultipleColumnFunction(function) {

  override def transform(columns: Column*): Column = concat(columns: _*)
}
