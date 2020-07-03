package edu.agus.epam

sealed trait OutputType
case class HDFS(path: String) extends OutputType
case class ElasticSearch(host: String, index: String) extends OutputType
