package edu.agus.data.processing

import edu.agus.data.OutputType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Processor {
  def process(spark: SparkSession, output: OutputType, kafkaDF: DataFrame)
}
