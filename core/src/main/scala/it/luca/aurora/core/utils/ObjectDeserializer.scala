package it.luca.aurora.core.utils

import com.fasterxml.jackson.databind.ObjectMapper
import it.luca.aurora.core.logging.Logging

object ObjectDeserializer
  extends Logging {

  private final val objectMapper: ObjectMapper = new ObjectMapper()

}
