package it.luca.aurora

import it.luca.aurora.core.logging.Logging
import it.luca.aurora.option.{CliArguments, ScoptOptionParser}

object Main
  extends App
    with Logging {

  log.info("Started application main class")
  ScoptOptionParser.parse(args, CliArguments()) match {
    case Some(arguments) =>

      log.info(s"Successfully parsed input arguments.\n\n$arguments")
      new DataLoadJob(arguments).run()
    case None => log.error(s"Unable to parse input arguments. Provided args: ${args.mkString(" ")}")
  }
}
