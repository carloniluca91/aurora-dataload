package it.luca.aurora.core.sql.functions
import net.sf.jsqlparser.expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when

abstract class CaseWhenFunction[K, V](override protected val function: expression.Function)
  extends SingleColumnFunction(function) {

  protected val casesMap: Map[K, V]

  override def transform(column: Column): Column = {

    val (firstKey, firstValue): (K, V) = casesMap.head
    val caseWhenSeed: Column = when(column === firstKey, firstValue)
    casesMap.tail.foldLeft(caseWhenSeed) {
      case (col, (key, value)) => col.when(column === key, value)
    }
  }
}
