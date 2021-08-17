package it.luca.aurora.option

case class CliArguments(yamlFileName: String = "N.A.",
                        dataSource: String = "N.A.") {

  override def toString: String = {

    val formatOption: (ScoptOption.Value, String) => String = (option, value) => s"  $option = $value"
    val options: Seq[String] = formatOption(ScoptOption.YamlFile, yamlFileName) ::
      formatOption(ScoptOption.DataSource, dataSource) :: Nil
    options.mkString("\n")
  }
}
