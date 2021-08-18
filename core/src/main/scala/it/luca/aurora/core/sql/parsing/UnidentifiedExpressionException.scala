package it.luca.aurora.core.sql.parsing

class UnidentifiedExpressionException(expression: String)
  extends RuntimeException(s"Unable to parse SQL statement '$expression'")
