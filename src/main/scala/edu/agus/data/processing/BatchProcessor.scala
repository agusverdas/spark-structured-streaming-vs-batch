package edu.agus.data.processing
import edu.agus.data
import edu.agus.data._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, datediff}
import edu.agus.data.implicits.ImplicitDFOperations._

import scala.annotation.tailrec

object BatchProcessor extends Processor {
  private val predicates = List(
    ErrorPredicate(),
    ShortPredicate(),
    StandardPredicate(),
    StandardExtendablePredicate(),
    LongPredicate()
  )

  override def process(spark: SparkSession, output: OutputType, kafkaDF: DataFrame): Unit = {
    val df2016 = spark.read.parquet(Util.Year2016Path)
    data.debugLog(
      "Expedia dataset was read(2016 year).",
      "Expedia data sample(2016 year)"
    )(df2016)
    val joined2016 = batchDataPrep(df2016, kafkaDF)
    debugLog(
      "DataFrame for 2016 with joined weather schema",
      "DataFrame for 2016 with joined weather"
    )(joined2016)

    val statesList = batchStates(joined2016, predicates)
    val state2016 = statesList.reduce((x, y) => x.join(y, Seq("hotel_id", "Name")))
    debugLog(
      "Schema for 2016 state",
      "2016 state"
    )(state2016)
    //TODO: output
  }

  @tailrec
  def batchStates(recursionBase: DataFrame, predicates: List[PredicateAlgebra], states: List[DataFrame] = Nil): List[DataFrame] = {
    predicates match {
      case x :: xs => batchStates(recursionBase, xs, recursionBase.batchStateAggregation(x) :: states)
      case Nil => states
    }
  }

  def batchDataPrep(bookingDf: DataFrame, hotelDf: DataFrame): DataFrame = {
    bookingDf.join(hotelDf, col("hotel_id") === hotelDf("Id")
      && col("srch_ci") === col("wthr_date"))
      // 32F = 0C
      .filter(col("avg_tempr_f") > 32)
      .withColumn("stay", datediff(col("srch_co"), col("srch_ci")))
      .drop("wthr_date")
      .drop(hotelDf("Id"))
  }
}
