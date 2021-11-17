package it.luca.aurora.app.job.transformer

case class TestClass(id: Int,
                     firstColumn: Option[String],
                     secondColumn: Option[String])

object TestClass {

  val Id = "id"
  val FirstColumn = "first_column"
  val SecondColumn = "second_column"
}
