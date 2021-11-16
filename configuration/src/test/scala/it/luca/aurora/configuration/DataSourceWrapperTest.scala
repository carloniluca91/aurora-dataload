package it.luca.aurora.configuration

import it.luca.aurora.configuration.datasource.DataSource
import it.luca.aurora.core.BasicTest

class DataSourceWrapperTest
  extends BasicTest {

  private val (dsId1, fileOfDsId1) = ("dsId1", "fileOfDsId1")
  private val dataSourcesMap: Map[String, String] = Map(dsId1 -> fileOfDsId1)
  private val dataSourceWrapper: DataSourceWrapper = DataSourceWrapper(dataSourcesMap)

  s"A ${nameOf[DataSourceWrapper]}" should s"correctly retrieve a ${nameOf[DataSource]}" in {

    val dataSource: DataSource = dataSourceWrapper.getDataSourceWithId(dsId1)
    dataSource.id shouldBe dsId1
    dataSource.metadataFilePath shouldBe fileOfDsId1
  }

  it should "throw an exception when an unknown id is requested" in {

    an [IllegalArgumentException] should be thrownBy dataSourceWrapper.getDataSourceWithId("unKnown")
  }
}
