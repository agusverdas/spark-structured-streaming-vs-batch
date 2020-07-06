package edu.agus.data.processing
import edu.agus.data.OutputType
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamProcessor extends Processor {
  override def process(spark: SparkSession, output: OutputType, kafkaDF: DataFrame): Unit = ???
}
