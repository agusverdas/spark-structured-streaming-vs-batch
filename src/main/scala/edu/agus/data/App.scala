package edu.agus.data

import edu.agus.data.processing.BatchProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

case class InputRow(hotel_id: Long, Name: String, stay: Long)
case class State(hotel_id: Long,
                 hotel: String,
                 var errorStay: Long = 0,
                 var shortStay: Long = 0,
                 var standartStay: Long = 0,
                 var standartExtendableStay: Long = 0,
                 var longStay: Long = 0)

object App {

  def readKafkaDf(spark: SparkSession): DataFrame = {
    val kafkaDF = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp:6667")
      .option("subscribe", "day_weather_hotel")
      .option("startingOffsets", "earliest")
      .load()
    val topicDF = kafkaDF.selectExpr("CAST(value AS STRING)")
    import spark.implicits._

    val schema = spark.read.json(topicDF.select("value").as[String]).schema
    val valueDF = topicDF.select(from_json(col("value"), schema).as("s")).select("s.*")
    debugLog(
      "Kafka dataset was read",
      "Kafka data sample"
    )(valueDF)

    valueDF
      .select(col("Id"), col("avg_tempr_f"), col("wthr_date"))
      .distinct()
  }

  /**
   * Main method.
   * @param args
   *             arg(0) - 'HDFS'/'ELK'
   *             arg(1) - If HDFS path to output directory. If ELK - host.
   *             arg(2) - If ELK index name.
   */
  def main(args: Array[String]) = {
    val output = Util.defineOutput(args)
    Util.withSpark(output) { spark =>
      val hotelWeatherDate: DataFrame = readKafkaDf(spark)
      BatchProcessor.process(spark, output, hotelWeatherDate)
      // Helps to infer schema from data format while streaming instead of schema manual creation
      spark.sql("set spark.sql.streaming.schemaInference=true")
      val df2017: DataFrame = spark.readStream.load(Util.Year2017Path)
      import spark.implicits._

      val joined2017 = df2017.join(hotelWeatherDate, col("hotel_id") === hotelWeatherDate("Id")
        && col("srch_ci") === col("wthr_date"))
        .filter(col("avg_tempr_f") > 32)
        .withColumn("stay", datediff(col("srch_co"), col("srch_ci")))
        .drop("wthr_date")
        .drop(hotelWeatherDate("Id"))
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
    }
  }

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
}
