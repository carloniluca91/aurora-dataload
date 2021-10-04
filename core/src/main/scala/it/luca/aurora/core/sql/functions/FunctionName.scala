package it.luca.aurora.core.sql.functions

object FunctionName extends Enumeration {

  type FunctionName = String

  val Concat = "concat"
  val ConcatWs = "concat_ws"
  val DateFormat = "date_format"
  val DecodeFlag = "decode_flag"
  val IsFlag = "is_flag"
  val LeftPad = "lpad"
  val LeftTrim = "ltrim"
  val MatchesDateFormat = "matches_date_format"
  val MatchesRegex = "matches_regex"
  val MatchesTimestampFormat = "matches_timestamp_format"
  val NeitherNullOrBlank = "neither_null_or_blank"
  val RegexExtract = "regex_extract"
  val RegexReplace = "regex_replace"
  val RightPad = "rpad"
  val RightTrim = "rtrim"
  val Substring = "substring"
  val ToDate = "to_date"
  val ToTimestamp = "to_timestamp"
  val Trim = "trim"
}
