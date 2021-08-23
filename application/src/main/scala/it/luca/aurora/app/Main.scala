package it.luca.aurora.app

import it.luca.aurora.app.job.DataloadJobRunner
import it.luca.aurora.core.logging.Logging
import it.luca.aurora.app.option.{CliArguments, CliOption}
import scopt.OptionParser

object Main
  extends App
    with Logging {

  log.info("Started application main class")
  val cliParser: OptionParser[CliArguments] = new OptionParser[CliArguments]("scopt 4.0") {

    // Yaml file option
    opt[String](CliOption.YamlFileName.shortOption, CliOption.YamlFileName.longOption)
      .text(CliOption.YamlFileName.description)
      .required()
      .validate(s => if (s.endsWith(".yaml")) success else failure(s"A .yaml file is expected. Found $s"))
      .action((s, c) => c.copy(yamlFileName = s))

    // DataSource option
    opt[String](CliOption.DataSource.shortOption, CliOption.DataSource.longOption)
      .text(CliOption.DataSource.description)
      .required()
      .action((s, c) => c.copy(dataSource = s))
  }

  // Parse command line arguments
  cliParser.parse(args, CliArguments()) match {
    case Some(arguments) =>

      log.info(s"Successfully parsed input arguments.\n\n$arguments")
      new DataloadJobRunner(arguments).run()

    case None => log.error(s"Unable to parse input arguments. Provided args: ${args.mkString(" ")}")
  }
}
