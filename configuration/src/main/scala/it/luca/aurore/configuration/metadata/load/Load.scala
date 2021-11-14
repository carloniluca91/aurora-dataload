package it.luca.aurore.configuration.metadata.load

import it.luca.aurore.configuration.Dto

case class Load(stagingPaths: StagingPaths,
                stagingTables: StagingTables)
  extends Dto

case class StagingPaths(success: String,
                        failed: String)
  extends Dto {

  required(success, "success")
  required(failed, "failed")
}

case class StagingTables(trusted: String,
                         error: String)
  extends Dto {

  required(trusted, "trusted")
  required(error, "error")
}
