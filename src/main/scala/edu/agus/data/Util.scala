package edu.agus.data

import org.apache.spark.sql.SparkSession

object Util {
  private val DefaultHDFSPath = "hdfs://sandbox-hdp:8020/user/hadoop/"
  val Year2016Path = s"${DefaultHDFSPath}modify/year=2016"
  val Year2017Path = s"${DefaultHDFSPath}modify/year=2017"

  val BatchOutput = s"${DefaultHDFSPath}streaming/batch"
  val StreamingOutput = s"${DefaultHDFSPath}streaming/stream"

  def withSpark(out: OutputType)(lambda: SparkSession => Unit) = {
    val spark: SparkSession =
      out match {
        case _: HDFS => SparkSession
          .builder()
          .appName("WriteToHDFS")
          .getOrCreate()
        case elk: ElasticSearch => SparkSession
          .builder()
          .appName("WriteToElasticSearch")
          .config("spark.es.nodes", elk.host)
          .config("spark.es.port", "9200")
          .getOrCreate()
      }

    try {
      lambda(spark)
    }
    finally spark.stop()
  }

  def defineOutput(args: Array[String]): OutputType = {
    val outputTypeText = args(0) toUpperCase
    val outputType: OutputType = outputTypeText match {
      case "HDFS" => HDFS(s"$DefaultHDFSPath${args(1)}")
      case "ELK" => ElasticSearch(host = args(1), index = args(2))
      case _ => throw new IllegalArgumentException("Invalid input source(app is working with 'ELK' or 'HDFS' sources)")
    }
    outputType
  }
}
