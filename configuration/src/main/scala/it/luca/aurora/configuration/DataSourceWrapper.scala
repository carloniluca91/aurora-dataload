package it.luca.aurora.configuration

import it.luca.aurora.configuration.datasource.DataSource

case class DataSourceWrapper(dataSources: Map[String, String]) {

  /**
   * Get path of dataSource with given id
   * @param id dataSource id
   * @throws IllegalArgumentException if no dataSource matches given id
   * @return instance of [[DataSource]]
   */

  @throws[IllegalArgumentException]
  def getDataSourceWithId(id: String): DataSource = {

    dataSources.collectFirst {
      case (id, filePath) if id.equalsIgnoreCase(id) => DataSource(id, filePath)
    } match {
      case Some(value) => value
      case None => throw new IllegalArgumentException(s"Unmatched dataSource id ($id)")
    }
  }
}
