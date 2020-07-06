package edu.agus

import org.apache.spark.sql.DataFrame

package object data {
  def debugLog(preSchemaLog: String, preDataLog: String)(df: DataFrame, rows: Int = 10) = {
    println(preSchemaLog)
    df.printSchema()
    println(preDataLog)
    df.show(rows)
  }
}
