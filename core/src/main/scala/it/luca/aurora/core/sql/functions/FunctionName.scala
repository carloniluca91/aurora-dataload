package it.luca.aurora.core.sql.functions

object FunctionName extends Enumeration {

  type FunctionName = String

  val Concat = "concat"
  val ConcatWs = "concat_ws"
  val DateFormat = "date_format"
  val IsBlank = "is_blank"
  val LeftPad = "lpad"
  val LeftTrim = "ltrim"
  val MatchesDateFormat = "matches_date_format"
  val MatchesTimestampFormat = "matches_timestamp_format"
  val RightPad = "rpad"
  val RightTrim = "rtrim"
  val Substring = "substring"
  val ToDate = "to_date"
  val ToTimestamp = "to_timestamp"
  val Trim = "trim"
}
