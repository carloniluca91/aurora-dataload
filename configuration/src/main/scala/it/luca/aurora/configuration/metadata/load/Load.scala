package it.luca.aurora.configuration.metadata.load

import it.luca.aurora.configuration.Dto

case class Load(stagingPath: StagingPath,
                stagingTable: StagingTable)
  extends Dto {

  required(stagingPath, "staginPath")
  required(stagingTable, "stagingTable")
}
