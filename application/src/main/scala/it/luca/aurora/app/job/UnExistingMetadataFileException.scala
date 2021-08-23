package it.luca.aurora.app.job

import it.luca.aurora.core.configuration.yaml.DataSource

class UnExistingMetadataFileException(protected val dataSource: DataSource)
  extends RuntimeException(s"Metadata file ${dataSource.getMetadataFilePath} for ${dataSource.getId} does not exist") {

}
