package cn.gaei.wh

import java.io.PrintWriter
import java.util.Collections

import ch.hsr.geohash.GeoHash
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.collections.map.LRUMap
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, count}
import org.geotools.geometry.jts.JTSFactoryFinder

import scala.io.Source

object Location {

  case class CITY(i:Int,pid:String, id:String,en:String,zh:String,poly:String)
  case class Name(id:String, city:String)
  case class LonLat(lat:Double,lon:Double)
  val code_city = collection.mutable.Map[String,Name]();
  import scala.collection.JavaConversions._
  def getName(dict:collection.mutable.Map[Int, CITY], id:Int): String = {
    var record = dict.get(id)
    var name = ""
    while(!record.isEmpty){
      name += ("-" + record.get.zh)
      val parent_record = dict.filter(e=>{
        e._2.id.equalsIgnoreCase(record.get.pid)
      })
      if(parent_record.size != 0){
        var i = parent_record.head._2.i
        parent_record.foreach(e=>{
          if(e._2.i < i){
            i = e._2.i
          }
        })
        record = dict.get(i)
      }else{
        record = None
      }
    }
    name.substring(1)
  }

  def getClosedRegion(code:String,point:Point,poly_to_id:collection.mutable.ArrayBuffer[(Int,Geometry,Geometry)],bval:collection.mutable.Map[Int,CITY]) = {
    var c = poly_to_id.filter(e=>{
      point.within(e._2)
    })

    var d = c.filter(e=>{
      point.within(e._3)
    })

    d = d.sortWith((e1,e2)=>{
      e1._1 > e2._1
    })

    if (d.size>0) {
      val i = d.head._1
      val city_id = bval.get(i).get.id
      val city_name = getName(bval ,i)
      Name(city_id, city_name)
    } else {
      Name("None", "None")
    }
  }

  def main(args: Array[String]): Unit = {

    val source = Source.fromFile(args(0),"utf8")
    val lines = source.getLines
    //map id -> city info
    val id_city = collection.mutable.Map[Int,CITY]()
    var i = 0
    for(line <- lines){

      val splits = line.split(";")
      val pid = splits(0)
      val id = splits(1)
      val en_name = splits(2)
      val zh_name = splits(3)
      val lvl = splits(4).toInt
      if(splits.length >5){
        val polygon = "POLYGON(("+splits(5)+"))"
        println(
          s"""
            |${id}
            |${pid}
            |${en_name}
            |${zh_name}
            |${polygon.subSequence(0,20)} ... ${polygon.substring(polygon.length - 20)}
          """.stripMargin)

        if (lvl < 7 ) {
          id_city += (i -> CITY(i, pid, id, en_name, zh_name, polygon))
          i = i+1;
        }

      }

    }
    //    println(mapping.mkString)
    source.close
    val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
    val reader = new WKTReader(geometryFactory)

//    val sc = new SparkContext(new SparkConf()).n
    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
//    val sc = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("spark.executor.memory", "8G")
//      .config("spark.executor.cores", "4")
//      .config("spark.driver.memory", "4")
//      .master("spark://master1:17077")
//      .getOrCreate()

    val broadcastVar = sc.sparkContext.broadcast(id_city)
    //val spark = sc.newSession()

    val poly_to_id = collection.mutable.ArrayBuffer[(Int,Geometry,Geometry)]()
    broadcastVar.value.foreach(item =>{
      val (id, city) = item
      val poly = reader.read(city.poly)
      val envelope = poly.getEnvelope()
      poly_to_id.append((id,envelope, poly))
    })

    import sc.implicits._
    sc.udf.register("geo", (lon: Double, lat: Double) =>{
      val r = GeoHash.geoHashStringWithCharacterPrecision(lat,lon,8)
      r
    })

    sc.udf.register("getCityName", (code:String) =>{

      val hash = GeoHash.fromGeohashString(code)
      val p = hash.getPoint()
      val coord = new Coordinate(p.getLongitude(), p.getLatitude());
      val point = geometryFactory.createPoint(coord);
      val res = getClosedRegion(code, point,poly_to_id, broadcastVar.value)
      res
      })

//    val out = new PrintWriter("./code_cnt.csv")
    val file = sc.read.parquet("/data/guobiao/parquet/d=20180418")
    val code_file = file
      .withColumn("loc_code",callUDF("geo",$"loc_lon84",$"loc_lat84"))

    val name_file = code_file.groupBy($"loc_code").agg(count("*").as("cnt"))
      .withColumn("name",callUDF("getCityName",$"loc_code"))

    code_file.join(name_file,"loc_code")
      //.withColumn("name",callUDF("getCityName",$"loc_code", $"loc_lon84",$"loc_lat84"))
      .withColumn("loc_city",$"name.city")
      .withColumn("loc_city_id",$"name.id")
      .drop("name")
      .write.parquet("/tmp/code1/")
//      .groupBy($"loc_code").agg(count("*").as("cnt")).sort($"cnt".desc).collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
//    out.close()
  }

}
