package it.luca.aurora.option

object ScoptOption extends Enumeration {

  protected case class ScoptOptionVal(shortOption: Char,
                                      longOption: String,
                                      description: String) extends super.Val {

    override def toString(): String = s"-$shortOption, --$longOption ($description)"
  }

  import scala.language.implicitConversions

  implicit def valueToEnumVal(x: Value): ScoptOptionVal = x.asInstanceOf[ScoptOptionVal]

  val PropertiesFile: ScoptOptionVal = ScoptOptionVal('p', "properties-file", "Name of .properties file for Spark application")
  val DataSource: ScoptOptionVal = ScoptOptionVal('d', "datasource", "Name of datasource for which data must be loaded")
}
