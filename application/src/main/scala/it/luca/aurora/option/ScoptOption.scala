package it.luca.aurora.option

object ScoptOption extends Enumeration {

  protected case class ScoptOptionVal(shortOption: Char,
                                      longOption: String,
                                      description: String) extends super.Val {

    override def toString(): String = s"-$shortOption, --$longOption ($description)"
  }

  import scala.language.implicitConversions

  implicit def valueToEnumVal(x: Value): ScoptOptionVal = x.asInstanceOf[ScoptOptionVal]

  val YamlFile: ScoptOptionVal = ScoptOptionVal('y', "yaml-file", "Name of .yaml file containing Spark application properties")
  val DataSource: ScoptOptionVal = ScoptOptionVal('d', "datasource", "Name of datasource for which data must be loaded")
}
