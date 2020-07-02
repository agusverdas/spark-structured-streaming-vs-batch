package edu.agus.epam

case class Pathable(path: String)

sealed trait OutputType
case class HDFS(path: String) extends OutputType
case class ElasticSearch(host: String, index: String) extends OutputType
