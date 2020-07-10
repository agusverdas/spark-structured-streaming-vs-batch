package edu.agus.data.processing
import edu.agus.data.{OutputType, Util}
import org.apache.spark.sql.functions.{col, datediff}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamProcessor extends Processor {
  case class InputRow(hotel_id: Long, Name: String, stay: Long)
  case class State(hotel_id: Long,
                   hotel: String,
                   var errorStay: Long = 0,
                   var shortStay: Long = 0,
                   var standartStay: Long = 0,
                   var standartExtendableStay: Long = 0,
                   var longStay: Long = 0)

  def updateStateWithEvent(state:State, input:InputRow):State = {
    input.stay match {
      case x if x <= 0 || x > 30 => state.errorStay = state.errorStay + 1
      case 1 => state.shortStay = state.shortStay + 1
      case x if x >= 2 && x <= 7 => state.standartStay = state.standartStay + 1
      case x if x > 7 && x < 14 => state.standartExtendableStay = state.standartExtendableStay + 1
      case _ => state.longStay = state.longStay + 1
    }
    state
  }

  def updateAcrossEvents(key: (Long, String),
                         inputs: Iterator[InputRow],
                         oldState: GroupState[State]): State = {
    var state:State = if (oldState.exists) oldState.get else State(key._1, key._2)

    for (input <- inputs) {
      state = updateStateWithEvent(state, input)
      oldState.update(state)
    }
    state
  }
  override def process(spark: SparkSession, output: OutputType, kafkaDF: DataFrame): Unit = {
    spark.sql("set spark.sql.streaming.schemaInference=true")
    val df2017: DataFrame = spark.readStream.load(Util.Year2017Path)
    import spark.implicits._

    val joined2017 = df2017.join(kafkaDF, col("hotel_id") === kafkaDF("Id")
      && col("srch_ci") === col("wthr_date"))
      .filter(col("avg_tempr_f") > 32)
      .withColumn("stay", datediff(col("srch_co"), col("srch_ci")))
      .drop("wthr_date")
      .drop(kafkaDF("Id"))
      .select("hotel_id", "Name", "stay")
      .as[InputRow]
      .groupByKey(r => (r.hotel_id, r.Name))
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)

    val query = joined2017
      .writeStream
      .outputMode("update")
      .format("parquet")
      .option("path", Util.StreamingOutput)
      .start
    query.awaitTermination()
    //TODO: Add output
  }
}
