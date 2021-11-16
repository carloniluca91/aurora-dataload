package it.luca.aurora.app.option

case class CliArguments(propertiesFileName: String = "N.A",
                        dataSourcesFileName: String = "N.A.",
                        dataSourceId: String = "N.A.") {

  override def toString: String = {

    val formatOption: (CliOption[_, CliArguments], String) => String = (option, value) => s"  $option = $value"
    val options: Seq[String] = formatOption(CliArguments.PropertiesFile, propertiesFileName) ::
      formatOption(CliArguments.DataSourcesFile, dataSourcesFileName) ::
      formatOption(CliArguments.DataSourceId, dataSourceId) :: Nil
    options.mkString("\n").concat("\n")
  }
}

object CliArguments extends CustomOptionParser[CliArguments] {

  val PropertiesFile: _RequiredWithValidation[String] = new _RequiredWithValidation[String] {
    override def validation: String => Either[String, Unit] =
      s => if (s.endsWith(".properties")) success else failure(s"A .properties file was expected. Found $s")
    override def shortOption: Char = 'p'
    override def longOption: String = "properties"
    override def description: String = "Properties file for Spark application"
    override def action: (String, CliArguments) => CliArguments = (s, c) => c.copy(propertiesFileName = s)
  }

  addOpt[String](PropertiesFile)

  val DataSourcesFile: _RequiredWithValidation[String] = new _RequiredWithValidation[String] {
    override def validation: String => Either[String, Unit] =
      s => if (s.endsWith(".json")) success else failure(s"A .json file was expected. Found $s")
    override def shortOption: Char = 'j'
    override def longOption: String = "json"
    override def description: String = "DataSource .json file for Spark application"
    override def action: (String, CliArguments) => CliArguments = (s, c) => c.copy(dataSourcesFileName = s)
  }

  addOpt[String](DataSourcesFile)

  val DataSourceId: _RequiredWithoutValidation[String] = new _RequiredWithoutValidation[String] {
    override def shortOption: Char = 'd'
    override def longOption: String = "dataSource"
    override def description: String = "Id of dataSource to trigger"
    override def action: (String, CliArguments) => CliArguments = (s, c) => c.copy(dataSourceId = s)
  }

  addOpt[String](DataSourceId)
}
