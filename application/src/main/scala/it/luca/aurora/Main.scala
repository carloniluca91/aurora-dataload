package it.luca.aurora

import it.luca.aurora.core.logging.Logging
import it.luca.aurora.option.{CliArguments, ScoptOption}
import scopt.OptionParser

object Main
  extends App
    with Logging {

  log.info("Started application main class")
  val cliParser: OptionParser[CliArguments] = new OptionParser[CliArguments]("scopt 4.0") {

    // Yaml file option
    opt[String](ScoptOption.YamlFileName.shortOption, ScoptOption.YamlFileName.longOption)
      .text(ScoptOption.YamlFileName.description)
      .required()
      .validate(s => if (s.endsWith(".yaml")) success else failure(s"A .yaml file is expected. Found $s"))
      .action((s, c) => c.copy(yamlFileName = s))

    // DataSource option
    opt[String](ScoptOption.DataSource.shortOption, ScoptOption.DataSource.longOption)
      .text(ScoptOption.DataSource.description)
      .required()
      .action((s, c) => c.copy(dataSource = s))
  }

  cliParser.parse(args, CliArguments()) match {
    case Some(arguments) =>

      log.info(s"Successfully parsed input arguments.\n\n$arguments")
      new DataLoadJob(arguments).run()

    case None => log.error(s"Unable to parse input arguments. Provided args: ${args.mkString(" ")}")
  }
}
