package it.luca.aurora.configuration.metadata

import it.luca.aurora.configuration.Dto
import it.luca.aurora.configuration.metadata.extract.Extract
import it.luca.aurora.configuration.metadata.load.Load
import it.luca.aurora.configuration.metadata.transform.Transform

/**
 * DataSource's metadata
 * @param id dataSource's id
 * @param extract dataSource's coordinates for data extraction
 * @param transform dataSource's specifications for transforming data
 * @param load dataSource's coordinates for loading data
 */

case class DataSourceMetadata(id: String,
                              extract: Extract,
                              transform: Transform,
                              load: Load)
  extends Dto {

  required(id, "id")
  required(extract, "extract")
  required(transform, "transform")
  required(load, "load")
}