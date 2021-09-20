package it.luca.aurora.app.option

object CliOption extends Enumeration {

  protected case class CliOptionVal(shortOption: Char,
                                    longOption: String,
                                    description: String) extends super.Val {

    override def toString(): String = s"-$shortOption, --$longOption ($description)"
  }

  import scala.language.implicitConversions

  implicit def valueToEnumVal(x: Value): CliOptionVal = x.asInstanceOf[CliOptionVal]

  val PropertiesFile: CliOptionVal = CliOptionVal('p', "properties", "Name of .properties file for Spark application")
  val YamlFile: CliOptionVal = CliOptionVal('y', "yaml", "Name of .yaml file with available datasources")
  val DataSourceId: CliOptionVal = CliOptionVal('d', "datasource", "Id of datasource for which data must be loaded")
}
