package edu.agus.data

import edu.agus.data.processing.{BatchProcessor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._



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
      //StreamProcessor.process(spark, output, hotelWeatherDate)
    }
  }
}
