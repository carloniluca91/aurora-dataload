package it.luca.aurora.app

import it.luca.aurora.app.job.DataloadJobRunner
import it.luca.aurora.app.option.CliArguments
import it.luca.aurora.core.Logging

object Main
  extends App
    with Logging {

  // Parse command line arguments
  log.info("Started application main class")
  CliArguments.parse(args, CliArguments()) match {
    case Some(arguments) =>
      log.info(s"Successfully parsed input arguments.\n\n$arguments")
      DataloadJobRunner.run(arguments)
    case None => log.error(s"Unable to parse input arguments. Provided args: ${args.mkString(" ")}")
  }
}
