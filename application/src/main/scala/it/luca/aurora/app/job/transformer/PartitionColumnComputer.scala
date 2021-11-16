package it.luca.aurora.app.job.transformer

import it.luca.aurora.configuration.metadata.transform.Partitioning
import org.apache.spark.sql.Column

/**
 * Base class defining a typed partition column computer
 * @param partitioning instance of a [[Partitioning]]'s subclass
 * @tparam P type of [[Partitioning]]'s subclass
 */

abstract class PartitionColumnComputer[P <: Partitioning](protected val partitioning: P) {

  /**
   * Get partition column
   * @return partition column
   */

  def getPartitionColumn: Column

}