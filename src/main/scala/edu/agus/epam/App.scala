package edu.agus.epam

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import edu.agus.epam.implicits.ImplicitDFOperations._

import scala.annotation.tailrec

case class InputRow(hotel_id: Long, Name: String, stay: Long)
case class State(hotel_id: Long,
                 hotel: String,
                 var errorStay: Long = 0,
                 var shortStay: Long = 0,
                 var standartStay: Long = 0,
                 var standartExtendableStay: Long = 0,
                 var longStay: Long = 0)

object App {
  private val predicateList = List(
    ErrorPredicate(),
    ShortPredicate(),
    StandardPredicate(),
    StandardExtendablePredicate(),
    LongPredicate()
  )

  @tailrec
  def batchStates(recursionBase: DataFrame, predicates: List[Predicate], states: List[DataFrame] = Nil): List[DataFrame] = {
    predicates match {
      case x :: xs => batchStates(recursionBase, xs, recursionBase.batchStateAggregation(x) :: states)
      case Nil => states
    }
  }

  private val DefaultHDFSPath = "hdfs://sandbox-hdp:8020/user/hadoop/"
  private val Year2016Path = s"${DefaultHDFSPath}modify/year=2016"
  private val Year2017Path = s"${DefaultHDFSPath}modify/year=2017"

  private val BatchOutput = s"${DefaultHDFSPath}streaming/batch"
  private val StreamingOutput = s"${DefaultHDFSPath}streaming/stream"

  def batchDataPrep(bookingDf: DataFrame, hotelDf: DataFrame): DataFrame = {
    bookingDf.join(hotelDf, col("hotel_id") === hotelDf("Id")
      && col("srch_ci") === col("wthr_date"))
      // 32F = 0C
      .filter(col("avg_tempr_f") > 32)
      .withColumn("stay", datediff(col("srch_co"), col("srch_ci")))
      .drop("wthr_date")
      .drop(hotelDf("Id"))
  }

  def main(args: Array[String]) = {
    val outputTypeText = args(0) toUpperCase
    val outputType: OutputType = outputTypeText match {
      case "HDFS" => HDFS(s"$DefaultHDFSPath${args(1)}")
      case "ELK" => ElasticSearch(host = args(1), index = args(2))
    }
    val sparkSession = SparkSession
      .builder()
      .appName("WriteToHDFS")
      .getOrCreate()
    val df2016 = sparkSession.read.parquet(Year2016Path)

    debugLog(
      "Expedia dataset was read(2016 year).",
      "Expedia data sample(2016 year)"
    )(df2016)

    // Helps to infer schema from data format while streaming instead of schema manual creation
    sparkSession.sql("set spark.sql.streaming.schemaInference=true")
    val df2017: DataFrame  = sparkSession.readStream.load(Year2017Path)


    val kafkaDF = sparkSession.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp:6667")
      .option("subscribe", "day_weather_hotel")
      .option("startingOffsets", "earliest")
      .load()
    val topicDF = kafkaDF.selectExpr("CAST(value AS STRING)")
    // Type classes for type casts
    import sparkSession.implicits._

    val schema = sparkSession.read.json(topicDF.select("value").as[String]).schema
    val valueDF = topicDF.select(from_json(col("value"), schema).as("s")).select("s.*")
    debugLog(
      "Kafka dataset was read",
      "Kafka data sample"
    )(valueDF)

    val hotelWeatherDate = valueDF
      .select(col("Id"), col("avg_tempr_f"), col("wthr_date"))
      // TODO: Not sure it really needs to distinct these rows
      .distinct()
    val joined2016 = batchDataPrep(valueDF, hotelWeatherDate)

    debugLog(
      "DataFrame for 2016 with joined weather schema",
      "DataFrame for 2016 with joined weather"
    )(joined2016)

    val statesList = batchStates(joined2016, predicateList)
    val state2016 = statesList.reduce((x, y) => x.join(y, Seq("hotel_id", "Name")))
    debugLog(
      "Schema for 2016 state",
      "2016 state"
    )(state2016)

    val joined2017 = df2017.join(hotelWeatherDate, col("hotel_id") === hotelWeatherDate("Id")
      && col("srch_ci") === col("wthr_date"))
      .filter(col("avg_tempr_f")  > 32)
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
      .option("path", StreamingOutput)
      .start
    query.awaitTermination()
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

  def debugLog(preSchemaLog: String, preDataLog: String)(df: DataFrame, rows: Int = 10) = {
    println(preSchemaLog)
    df.printSchema()
    println(preDataLog)
    df.show(rows)
  }
}
