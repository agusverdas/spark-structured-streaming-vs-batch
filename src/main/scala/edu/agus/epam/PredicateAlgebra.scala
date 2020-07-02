package edu.agus.epam

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

abstract class Predicate(val predicate: Column)

sealed trait PredicateAlgebra
case class ErrorPredicate() extends Predicate(col("stay") <= 0 or col("stay") > 30) with PredicateAlgebra
case class ShortPredicate() extends Predicate(col("stay") === 1) with PredicateAlgebra
case class StandardPredicate() extends Predicate(col("stay") >= 2 && col("stay") <= 7) with PredicateAlgebra
case class StandardExtendablePredicate() extends Predicate(col("stay") > 7 && col("stay") < 14) with PredicateAlgebra
case class LongPredicate() extends Predicate(col("stay") >= 14 && col("stay") <= 28) with PredicateAlgebra
