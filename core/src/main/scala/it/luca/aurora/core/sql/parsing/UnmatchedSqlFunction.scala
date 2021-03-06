package it.luca.aurora.core.sql.parsing

import net.sf.jsqlparser.expression

class UnmatchedSqlFunction(function: expression.Function)
  extends Exception(s"Unable to match function name '${function.getName}'")
