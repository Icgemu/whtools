package cn.gaei.ev.wh.gb

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

object DataLossChecker extends Logging{

  val logger = LoggerFactory.getLogger(DataLossChecker.getClass)
  case class Segment(vin:String, ts:Long, st:Int)

  def tripst(e: Seq[Row]): Int = {
    //val e = m.map(_.map(Option(_)))
    var f = 0
    if (e.size == 2) {
      val t1 = e(0).getLong(0)
      val t2 = e(1).getLong(0)

      val s1 =e(0).getInt(1)
      val s2 = e(1).getInt(1)

      val tolerance = t2 - t1 < 300 * 1000

      if (tolerance && (s1 != 1) && (s2 == 1)) {
        f = 1//OD start
      } else if (tolerance && (s1 == 1) && (s2 == 1)) {
        f = 2//OD
      } else if (tolerance && (s2 != 1) && (s1 == 1)) {
        f = 3//OD stop
      } else if (!tolerance) {
        if(s2 == 1){f = 1}else{f = 3}
      }
    }
    //first point
    if (e.size == 1) {
      val s1 = e(0).getInt(1)
      if (s1 == 1) {
        f = 2
      }
    }
    f
  }

  def tripSegment(e: Seq[Segment], map:collection.mutable.Map[String,Int]): Int = {
    val vin = e(0).vin
    val f1 = -1
    var f2 = f1
    // ignore the first charging point that span two day...
    if (e.size == 1) {
      val s1 = e(0).st
      if (s1 == 2) {
        f2 = f1
      }
    }
    if (e.size == 2) {
      val s1 = e(0).st
      val s2 = e(1).st

      if (s2 == 1){
        f2 = map.getOrElse(vin, 0)+1
        map += (vin -> f2)

        println(map.mkString(","))
      }
      if (s2 == 2) f2 = map.getOrElse(vin, 0)
      if (s1 == 2 && s2 == 3) f2 = map.getOrElse(vin, 0)
    }

    f2
  }

  def tripLoss(e: Seq[Row]): Int = {

    var f = 0
    if (e.size == 2) {
      val t1 = e(0).getLong(0)
      val t2 = e(1).getLong(0)

      val o1 = e(0).getDouble(1)
      val o2 = e(1).getDouble(1)

      val f1 = e(0).getInt(2)
      val f2 = e(1).getInt(2)
      if(f2 != f1){

        val sp = (o2 - o1 )/((t2 - t1) / 1000.0) / 3600.0
        if(sp > 10){
          f = 1
        }
      }
    }
    f
  }


  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
    val map = collection.mutable.Map[String,Int]()
    val b_map = sc.sparkContext.broadcast(map)
    sc.udf.register("tripst", (e: Seq[Row]) => {
      tripst(e)
    })

    sc.udf.register("tripSegment1", (e: Seq[Row]) => {
      println(e.mkString(","))
      val arr = e.map(a => {Segment(a.getString(0),a.getLong(1) ,a.getInt(2))})
      //println(arr.mkString(","))
      tripSegment(arr,b_map.value)
    })

    sc.udf.register("tripLoss", (e: Seq[Row]) => {
      tripLoss(e)
    })

    val out = new PrintWriter("./LMGHP1S81H1000113.csv")
    val data = sc.read.parquet("/data/gb/parquet/*")

    import sc.implicits._

    val ws1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 0)
    val ws2 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 0)
    val ws3 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 0)

    data
      .filter($"vin".equalTo("LMGHP1S81H1000113"))
      .filter($"d".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271 && $"veh_odo"> 0)
      .withColumn("keyst", when($"veh_st".equalTo(1), 1).otherwise(0))
      .withColumn("tripst", callUDF("tripst", collect_list(struct($"ts", $"keyst")).over(ws1)))
      .withColumn("tripId", callUDF("tripSegment1", collect_list(struct($"vin",$"ts",$"tripst")).over(ws2)))
      .withColumn("loss",callUDF("tripLoss", collect_list(struct($"ts",$"veh_odo",$"tripId")).over(ws3)))
      .withColumn("date_str",from_unixtime($"ts"/1000,"yyyy-MM-dd"))
      .select($"date_str",$"ts",from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),$"loc_lon84",$"loc_lat84",$"veh_odo",$"veh_st", $"keyst",$"tripst",$"tripId",$"loss")
      .sort($"ts")
//      .explain(true)
      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
//      .groupBy($"date_str").agg(sum($"loss").as("loss_cnt"))
//      .sort($"date_str")
//      .collect().foreach(e=>{println(e.mkString(","))})

    out.close()
//    print(map)
  }

}
