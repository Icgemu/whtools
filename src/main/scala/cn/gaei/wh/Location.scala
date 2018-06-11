package cn.gaei.wh

import ch.hsr.geohash.GeoHash
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{callUDF, count}
import org.geotools.geometry.jts.JTSFactoryFinder

import scala.io.Source
import scala.reflect.ClassTag

object Location {

  private[this] case class CITY(i: Int, pid: String, id: String, en: String, zh: String, poly: String)
  private[this] case class Name(id: String, city: String)
  private[this] case class LonLat(lat: Double, lon: Double)

  //private[this] var __id_city = collection.mutable.Map[Int, CITY]()
  //private[this] var __poly_to_id = collection.mutable.ArrayBuffer[(Int, Geometry, Geometry)]()
  private[this] val __geometryFactory = JTSFactoryFinder.getGeometryFactory(null)

  private[this] def _readBoundary(path: String): collection.mutable.Map[Int, CITY] = {
    val source = Source.fromFile(path, "utf8")
    val lines = source.getLines
    //map id -> city info
    var i = 0
    var __id_city = collection.mutable.Map[Int, CITY]()
    for (line <- lines) {

      val splits = line.split(";")
      val pid = splits(0)
      val id = splits(1)
      val en_name = splits(2)
      val zh_name = splits(3)
      val lvl = splits(4).toInt
      if (splits.length > 5) {
        val polygon = "POLYGON((" + splits(5) + "))"
//        println(
//          s"""
//             |${id}
//             |${pid}
//             |${en_name}
//             |${zh_name}
//             |${polygon.subSequence(0, 20)} ... ${polygon.substring(polygon.length - 20)}
//          """.stripMargin)

        if (lvl < 7) {
          __id_city += (i -> CITY(i, pid, id, en_name, zh_name, polygon))
          i = i + 1;
        }

      }
    }

    __id_city
  }


  private[this] def _getName(id: Int, __id_city : collection.mutable.Map[Int, CITY]): String = {
    var record = __id_city.get(id)
    var name = ""
    while (!record.isEmpty) {
      name += ("-" + record.get.zh)
      val parent_record = __id_city.filter(e => {
        e._2.id.equalsIgnoreCase(record.get.pid)
      })
      if (parent_record.size != 0) {
        var i = parent_record.head._2.i
        parent_record.foreach(e => {
          if (e._2.i < i) {
            i = e._2.i
          }
        })
        record = __id_city.get(i)
      } else {
        record = None
      }
    }
    name.substring(1)
  }

  private[this] def _getClosedRegion(code: String, point: Point,
                                     __poly_to_id: collection.mutable.ArrayBuffer[(Int, Geometry, Geometry)],
                                     __id_city : collection.mutable.Map[Int, CITY]) = {
    val c = __poly_to_id.filter(e => {
      point.within(e._2)
    })

    var d = c.filter(e => {
      point.within(e._3)
    })

    d = d.sortWith((e1, e2) => {
      e1._1 > e2._1
    })

    if (d.size > 0) {
      val i = d.head._1
      val city_id = __id_city.get(i).get.id
      val city_name = _getName(i,__id_city)
      Name(city_id, city_name)
    } else {
      Name("None", "None")
    }
  }

  def setLocation[T: ClassTag](ds: Dataset[T], locationIdName: String, LocationCityName: String, lat: Column, lon: Column): DataFrame = {
    val sc = ds.sparkSession

    val boundary_path = sc.conf.get("wh.loc.boundary.path", SparkFiles.get("rel_path_added.txt"))
    val b_val = sc.sparkContext.broadcast(_readBoundary(boundary_path))
    val reader = new WKTReader(__geometryFactory)
    val __poly_to_id = collection.mutable.ArrayBuffer[(Int, Geometry, Geometry)]()
    b_val.value.foreach(item => {
      val (id, city) = item
      val poly = reader.read(city.poly)
      val envelope = poly.getEnvelope()
      __poly_to_id.append((id, envelope, poly))
    })

    import sc.implicits._
    sc.udf.register("geo", (lat: Double, lon: Double) => {
      GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 7)
    })

    sc.udf.register("getCityName", (code: String) => {

      val hash = GeoHash.fromGeohashString(code)
      val p = hash.getPoint()
      val coord = new Coordinate(p.getLongitude(), p.getLatitude())
      val point = __geometryFactory.createPoint(coord)
      val res = _getClosedRegion(code, point, __poly_to_id, b_val.value)
      res
    })

    val __code_file = ds
      .withColumn("__loc_code", callUDF("geo", lat, lon))

    val __name_file = __code_file.groupBy($"__loc_code").agg(count("*").as("cnt"))
      .withColumn("__name", callUDF("getCityName", $"__loc_code"))

    __code_file.join(__name_file, "__loc_code")
      .withColumn(LocationCityName, $"__name.city")
      .withColumn(locationIdName, $"__name.id")
      .drop("__name", "__loc_code")
  }

}
