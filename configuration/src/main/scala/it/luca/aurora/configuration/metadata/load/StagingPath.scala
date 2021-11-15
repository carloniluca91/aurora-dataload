package it.luca.aurora.configuration.metadata.load

import it.luca.aurora.configuration.Dto

case class StagingPath(success: String,
                       failed: String)
  extends Dto {

  required(success, "success")
  required(failed, "failed")
}
