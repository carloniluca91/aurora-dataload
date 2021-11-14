package it.luca.aurora.configuration.metadata

import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.load.Load
import it.luca.aurora.configuration.metadata.transform.Transform

case class DataSourceMetadata(id: String,
                              extract: Extract,
                              transform: Transform,
                              load: Load)