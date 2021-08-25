package it.luca.aurora.core.configuration.metadata

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class DataSourceColumnTest
  extends AnyFlatSpec
    with should.Matchers {

  s"A ${classOf[DataSourceColumn].getSimpleName}" should
    s"throw an ${classOf[IllegalArgumentException].getSimpleName} in case of an unknown datatype" in {

    an [IllegalArgumentException] should be thrownBy {
      new DataSourceColumn("name", "undefinedType")
    }
  }

  it should s"be correctly converted to a ${classOf[StructField].getSimpleName}" in {

    val (name, cType): (String, String) = ("name", "string")
    val structField = new DataSourceColumn(name, cType).toStructField
    structField.name shouldEqual name
    structField.dataType shouldEqual DataTypes.StringType
    structField.nullable shouldEqual true
  }
}
