package edu.agus.data

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

sealed trait PredicateAlgebra {
  val predicate: Column
}
case class ErrorPredicate() extends PredicateAlgebra {
  override val predicate: Column = col("stay") <= 0 or col("stay") > 30
}
case class ShortPredicate() extends PredicateAlgebra {
  override val predicate: Column = col("stay") === 1
}
case class StandardPredicate() extends PredicateAlgebra {
  override val predicate: Column = col("stay") >= 2 && col("stay") <= 7
}
case class StandardExtendablePredicate() extends PredicateAlgebra {
  override val predicate: Column = col("stay") > 7 && col("stay") < 14
}
case class LongPredicate() extends PredicateAlgebra {
  override val predicate: Column = col("stay") >= 14 && col("stay") <= 28
}
