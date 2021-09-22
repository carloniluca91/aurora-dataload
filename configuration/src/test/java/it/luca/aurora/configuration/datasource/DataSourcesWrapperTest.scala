package it.luca.aurora.configuration.datasource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util

class DataSourcesWrapperTest
  extends AnyFlatSpec
  with should.Matchers {

  s"An ${classOf[DataSourcesWrapper].getSimpleName}" should
    s"throw an ${classOf[UnExistingDataSourceException].getSimpleName} if an unexisting dataSourceId is provided" in {

    val dataSourceMap: util.Map[String, String] = new util.HashMap[String, String]() {{
      put("ID1", "metadata/file/path.json")
    }}
    val wrapper = new DataSourcesWrapper(dataSourceMap)
    an [UnExistingDataSourceException] should be thrownBy {
      wrapper.getDataSourceWithId("unexistingId")
    }
  }
}