package it.luca.aurora.app.option

object CliOption extends Enumeration {

  protected case class CliOptionVal(shortOption: Char,
                                    longOption: String,
                                    description: String) extends super.Val {

    override def toString(): String = s"-$shortOption, --$longOption ($description)"
  }

  import scala.language.implicitConversions

  implicit def valueToEnumVal(x: Value): CliOptionVal = x.asInstanceOf[CliOptionVal]

  val YamlFileName: CliOptionVal = CliOptionVal('y', "yaml-file", "Name of .yaml file for Spark application")
  val DataSource: CliOptionVal = CliOptionVal('d', "datasource", "Name of datasource for which data must be loaded")
}
