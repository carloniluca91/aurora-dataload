package it.luca.aurora.app

import it.luca.aurora.app.job.DataloadJobRunner
import it.luca.aurora.app.option.{CliArguments, CliOption}
import it.luca.aurora.core.Logging
import scopt.OptionParser

object Main
  extends App
    with Logging {

  log.info("Started application main class")
  val cliParser: OptionParser[CliArguments] = new OptionParser[CliArguments]("scopt 4.0") {

    // Properties file option
    opt[String](CliOption.PropertiesFile.shortOption, CliOption.PropertiesFile.longOption)
      .text(CliOption.PropertiesFile.description)
      .required()
      .validate(s => if (s.endsWith(".properties")) success else failure(s"A .properties file was expected. Found $s"))
      .action((s, c) => c.copy(propertiesFileName = s.trim))

    // Yaml file option
    opt[String](CliOption.YamlFile.shortOption, CliOption.YamlFile.longOption)
      .text(CliOption.YamlFile.description)
      .required()
      .validate(s => if (s.endsWith(".yaml")) success else failure(s"A .yaml file was expected. Found $s"))
      .action((s, c) => c.copy(yamlFileName = s.trim))

    // DataSource option
    opt[String](CliOption.DataSourceId.shortOption, CliOption.DataSourceId.longOption)
      .text(CliOption.DataSourceId.description)
      .required()
      .action((s, c) => c.copy(dataSourceId = s.trim))
  }

  // Parse command line arguments
  cliParser.parse(args, CliArguments()) match {
    case Some(arguments) =>

      log.info(s"Successfully parsed input arguments.\n\n$arguments")
      new DataloadJobRunner().run(arguments)

    case None => log.error(s"Unable to parse input arguments. Provided args: ${args.mkString(" ")}")
  }
}
