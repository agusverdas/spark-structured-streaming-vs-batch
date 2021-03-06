package edu.agus.data.implicits

import edu.agus.data.{ErrorPredicate, LongPredicate, PredicateAlgebra, ShortPredicate, StandardExtendablePredicate, StandardPredicate}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Column, DataFrame}

object ImplicitDFOperations {
  implicit class ImplicitDF(df: DataFrame) {
    def batchStateAggregation(predicateObj: PredicateAlgebra) = {
      val colName = predicateObj match {
        case _: ErrorPredicate => "errorStay"
        case _: ShortPredicate => "shortStay"
        case _: StandardPredicate => "standardStay"
        case _: StandardExtendablePredicate => "standardExtendableStay"
        case _: LongPredicate => "longStay"
      }

      df
        .select("hotel_id", "Name", "stay")
        .where(predicateObj.predicate)
        .groupBy("hotel_id", "Name")
        .agg(count("stay") as colName)
        .drop("stay")
        .distinct()
        .toDF()
    }
  }
}
