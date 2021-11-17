package it.luca.aurora.app.option

import scopt.{OptionDef, OptionParser, Read}

/**
 * Abstract parser for a generic case class of type C collecting command line argument
 * @tparam C type of case class collecting command line argument
 */

abstract class CustomOptionParser[C <: Product]
  extends OptionParser[C]("scopt 4.0") {

  type _CliOption[A] = CliOption[A, C]
  type _RequiredWithValidation[A] = RequiredWithValidation[A, C]
  type _RequiredWithoutValidation[A] = RequiredWithoutValidation[A, C]

  protected def addOpt[A](cliOption: _CliOption[A])(implicit evidence$2: Read[A]): OptionDef[A, C] = {

    val basicOptionDef: OptionDef[A, C] = opt[A](cliOption.shortOption, cliOption.longOption)
      .text(cliOption.description)

    val optionMaybeRequired: OptionDef[A, C] = if (cliOption.required) basicOptionDef.required() else basicOptionDef.optional()
    cliOption.optionalValidation
      .map{optionMaybeRequired.validate}
      .getOrElse(optionMaybeRequired)
      .action(cliOption.action)
  }
}