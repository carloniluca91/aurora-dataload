package it.luca.aurore.configuration.metadata

import it.luca.aurore.configuration.metadata.extract.Extract
import it.luca.aurore.configuration.metadata.load.Load
import it.luca.aurore.configuration.metadata.transform.Transform

case class DataSourceMetadata(id: String,
                              extract: Extract,
                              transform: Transform,
                              load: Load)