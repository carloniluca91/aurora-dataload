package it.luca.aurora.app.option

case class CliArguments(yamlFileName: String = "N.A.",
                        dataSource: String = "N.A.") {

  override def toString: String = {

    val formatOption: (CliOption.Value, String) => String = (option, value) => s"  $option = $value"
    val options: Seq[String] = formatOption(CliOption.YamlFileName, yamlFileName) ::
      formatOption(CliOption.DataSource, dataSource) :: Nil
    options.mkString("\n")
  }
}
