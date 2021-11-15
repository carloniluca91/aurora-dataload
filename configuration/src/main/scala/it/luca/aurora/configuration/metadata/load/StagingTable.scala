package it.luca.aurora.configuration.metadata.load

import it.luca.aurora.configuration.Dto

case class StagingTable(trusted: String,
                        error: String)
  extends Dto {

  required(trusted, "trusted")
  required(error, "error")
}
