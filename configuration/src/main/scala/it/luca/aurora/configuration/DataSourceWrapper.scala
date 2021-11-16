package it.luca.aurora.configuration

import it.luca.aurora.configuration.datasource.DataSource

case class DataSourceWrapper(dataSources: Map[String, String]) {

  /**
   * Get path of dataSource with given id
   * @param dataSourceId dataSource id
   * @throws IllegalArgumentException if no dataSource matches given id
   * @return instance of [[DataSource]]
   */

  @throws[IllegalArgumentException]
  def getDataSourceWithId(dataSourceId: String): DataSource = {

    dataSources.find {
      case (id, _) => id.equalsIgnoreCase(dataSourceId)
    } match {
      case Some((id, filePath)) => DataSource(id, filePath)
      case None => throw new IllegalArgumentException(s"Unmatched dataSource id ($dataSourceId)")
    }
  }
}
