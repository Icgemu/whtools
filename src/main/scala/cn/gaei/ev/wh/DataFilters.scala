package cn.gaei.ev.wh

import ch.hsr.geohash.GeoHash
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, count}
import org.geotools.geometry.jts.JTSFactoryFinder

import scala.io.Source

object DataFilters {


  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
    val gb = sc.read.parquet("/data/guobiao/parquet/")
    val ag = sc.read.parquet("/data/AG/parquet/")

    import sc.implicits._
    gb.filter($"vin".isin("LMGFJ1S89H1000353","LMGHP1S81H1000113","LMGAJ1S88H1000547"))
        .repartition(1)
      .write.parquet("/tmp/filter/gb/parquet")
    ag.filter($"vin".isin("LMGGN1S55F1000510","LMGGN1S52E1000155","LMGGN1S57F1000511"))
        .repartition(1)
      .write.parquet("/tmp/filter/ag/parquet")
  }

}
