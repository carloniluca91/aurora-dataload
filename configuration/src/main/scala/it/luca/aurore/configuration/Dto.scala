package it.luca.aurore.configuration

abstract class Dto {
  
  def required[T](instance: T, fieldName: String): Unit = {

    Option(instance) match {
      case Some(_) =>
      case None => throw new IllegalArgumentException(s"Field $fieldName of class ${this.getClass.getSimpleName} is not defined")
    }
  }

  def requiredNotEmpty[T](seq: Seq[T], fieldName: String): Unit = {

    if (seq.isEmpty)
      throw new IllegalArgumentException(f"List $fieldName of class ${this.getClass.getSimpleName} is empty")
  }
}
