package it.luca.aurora.app.option

import it.luca.aurora.core.BasicTest

class CliArgumentsTest
  extends BasicTest {

  private val PropertiesFile = "file.properties"
  private val DataSourcesFile = "dataSources.json"
  private val DataSourceId = "DS_IS"
  private val optionMap: Map[CliOption[_, CliArguments], String] = Map(
    CliArguments.PropertiesFile -> PropertiesFile,
    CliArguments.DataSourcesFile -> DataSourcesFile,
    CliArguments.DataSourceId -> DataSourceId
  )

  private def cliOptionMapToArgs(optionMap: Map[CliOption[_, CliArguments], String])
                                (function: CliOption[_, CliArguments] => String): Seq[String] = {

    optionMap.flatMap {
      case (cliOption, value) => function(cliOption) :: value :: Nil
    }.toSeq
  }

  s"A ${nameOf[CliArguments]}" should "be correctly initialized given short option argument" in {

    val args: Seq[String] = cliOptionMapToArgs(optionMap)(x => s"-${x.shortOption.toString}")
    val cliArgumentsOpt: Option[CliArguments] = CliArguments.parse(args, CliArguments())
    cliArgumentsOpt shouldBe Some(_: CliArguments)
    val cliArguments: CliArguments = cliArgumentsOpt.get
    cliArguments.propertiesFileName shouldBe PropertiesFile
    cliArguments.dataSourcesFileName shouldBe DataSourcesFile
    cliArguments.dataSourceId shouldBe DataSourceId
  }

  it should "be correctly initialized given long option argument" in {

    val args: Seq[String] = cliOptionMapToArgs(optionMap)(x => s"--${x.longOption}")
    val cliArgumentsOpt: Option[CliArguments] = CliArguments.parse(args, CliArguments())
    cliArgumentsOpt shouldBe Some(_: CliArguments)
    val cliArguments: CliArguments = cliArgumentsOpt.get
    cliArguments.propertiesFileName shouldBe PropertiesFile
    cliArguments.dataSourcesFileName shouldBe DataSourcesFile
    cliArguments.dataSourceId shouldBe DataSourceId
  }
}
