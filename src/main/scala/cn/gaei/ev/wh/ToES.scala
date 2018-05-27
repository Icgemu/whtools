package cn.gaei.ev.wh

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

object ToES {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
    import sc.implicits._

    val gb = sc.read.parquet("/data/gb/parquet/").filter($"ts" < 1608318715866l)
    val ag = sc.read.parquet("/data/ag/parquet/").filter($"ts" < 1608318715866l)

    val es_cfg = Map("es.nodes"->"localhost:9200,localhost:9201","es.mapping.timestamp" -> "ts",
      "es.batch.size.bytes" -> "10mb","es.batch.size.entries" -> "10000")



    EsSparkSQL.saveToEs(gb,"ec.2018/GB",es_cfg)
    EsSparkSQL.saveToEs(ag,"ec.2018/AG",es_cfg)
  }

}
