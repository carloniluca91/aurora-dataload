package it.luca.aurora

import it.luca.aurora.logging.Logging
import it.luca.aurora.option.{CliArguments, ScoptOptionParser}

object Main
  extends App
    with Logging {

  log.info("Started application main class")
  ScoptOptionParser.parse(args, CliArguments()) match {
    case Some(x) => log.info(s"Successfully parsed input arguments.\n\n$x")
    case None => log.error(s"Unable to parse input arguments. Provided args: ${args.mkString(" ")}")
  }
}
