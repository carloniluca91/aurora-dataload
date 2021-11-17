package it.luca.aurora.app.option

/**
 * Basic option for parsing an argument
 * @tparam A type of argument related to this option
 * @tparam C type of class collecting parsed arguments
 */

sealed trait CliOption[A, C] {

  def shortOption: Char

  def longOption: String

  def description: String

  def required: Boolean

  protected[option] def optionalValidation: Option[A => Either[String, Unit]]

  def action: (A, C) => C

  override def toString: String = s"-$shortOption, --$longOption ($description)"
}

/**
 * Option with required flag set to true
 * @tparam A type of argument related to this option
 * @tparam C type of class collecting parsed arguments
 */

sealed trait Required[A, C]
  extends CliOption[A, C] {

  override def required: Boolean = true
}

/**
 * Option that defines a function for argument validation
 * @tparam A type of argument related to this option
 * @tparam C type of class collecting parsed arguments
 */

sealed trait WithValidation[A, C]
  extends CliOption[A, C] {

  override def optionalValidation: Option[A => Either[String, Unit]] = Some(validation)

  def validation: A => Either[String, Unit]
}

/**
 * Option that does not apply validation to related argument
 * @tparam A type of argument related to this option
 * @tparam C type of class collecting parsed arguments
 */

sealed trait WithoutValidation[A, C]
  extends CliOption[A, C] {

  override protected[option] def optionalValidation: Option[A => Either[String, Unit]] = None
}

trait RequiredWithoutValidation[A, C]
  extends Required[A, C]
    with WithoutValidation[A, C]

trait RequiredWithValidation[A, C]
  extends Required[A, C]
    with WithValidation[A, C]