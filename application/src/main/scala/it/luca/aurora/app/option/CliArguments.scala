package it.luca.aurora.app.option

case class CliArguments(propertiesFileName: String = "N.A",
                        dataSourcesFileName: String = "N.A.",
                        dataSourceId: String = "N.A.") {

  override def toString: String = {

    val formatOption: (CliOption.Value, String) => String = (option, value) => s"  $option = $value"
    val options: Seq[String] = formatOption(CliOption.PropertiesFile, propertiesFileName) ::
      formatOption(CliOption.DataSourcesFile, dataSourcesFileName) ::
      formatOption(CliOption.DataSourceId, dataSourceId) :: Nil
    options.mkString("\n").concat("\n")
  }
}
