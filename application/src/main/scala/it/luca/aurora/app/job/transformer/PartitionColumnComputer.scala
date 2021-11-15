package it.luca.aurora.app.job.transformer

import it.luca.aurora.core.Logging
import org.apache.spark.sql.Column

/**
 * Trait to be implemented for computing a partition column
 * @tparam T type of input needed for partition column computation
 */

trait PartitionColumnComputer[T]
  extends Logging {

  /**
   * Get partition column
   * @param input instance of input needed for partition column computation
   * @return partition column
   */

  def getPartitionColumn(input: T): Column

}