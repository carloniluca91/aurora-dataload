package it.luca.aurora.configuration

package object implicits {

  implicit def toStringInterpolator(string: String): StringInterpolator = new StringInterpolator(string)

}
