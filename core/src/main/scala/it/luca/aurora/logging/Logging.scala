package it.luca.aurora.logging

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  protected final val log: Logger = LoggerFactory.getLogger(this.getClass)

}
