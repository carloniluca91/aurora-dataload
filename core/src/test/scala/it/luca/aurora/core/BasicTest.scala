package it.luca.aurora.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe.{typeOf, TypeTag}

trait BasicTest
  extends AnyFlatSpec
    with should.Matchers {

  protected final def nameOf[T: TypeTag]: String = typeOf[T].typeSymbol.name.toString

}