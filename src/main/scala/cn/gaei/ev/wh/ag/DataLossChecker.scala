package cn.gaei.ev.wh.ag

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object DataLossChecker {

  case class Segment(vin:String, st:Int)

  val map = collection.mutable.Map[String,Int]()

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

  def tripSegment(e: Seq[Segment]): Int = {

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
      }
      if (s2 == 2) f2 = map.getOrElse(vin, 0)
      //if (s1 == 2 && s2 == 3) f2 = map.getOrElse(vin, 0)
    }

    f2
  }

  def tripLoss(e: Seq[Row]): Int = {

    var f = 0
    if (e.size == 2) {
      val t1 = e(0).getLong(0)
      val t2 = e(1).getLong(0)

      val o1 = e(0).getInt(1)
      val o2 = e(1).getInt(1)

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

    sc.udf.register("tripst", (e: Seq[Row]) => {
      tripst(e)
    })

    sc.udf.register("tripSegment", (e: Seq[Row]) => {
      val arr = e.map(e => {Segment(e.getString(0), e.getInt(1))})
      tripSegment(arr)
    })
    sc.udf.register("tripLoss", (e: Seq[Row]) => {
      //val arr = e.map(e => {Segment(e.getString(0), e.getInt(1))})
      tripLoss(e)
    })

    val data = sc.read.parquet("/data/AG/parquet/d=2018*")

    import sc.implicits._

    //val out = new PrintWriter("./LMGGN1S55F1000510.csv")
    val ws1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 0)

    data
      //.filter($"vin".equalTo("LMGGN1S55F1000510"))
      //.filter($"date_str".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"icm_totalodometer" > 0)
      .withColumn("keyst", when(($"bcm_keyst".isNull) || $"bcm_keyst".equalTo(0), 0).otherwise(1))
      .withColumn("tripst", callUDF("tripst", collect_list(struct($"ts", $"keyst")).over(ws1)))
      .withColumn("tripId", callUDF("tripSegment", collect_list(struct($"vin",$"tripst")).over(ws1)))
      .withColumn("loss",callUDF("tripLoss", collect_list(struct($"ts",$"icm_totalodometer",$"tripId")).over(ws1)))
      //.groupBy($"date_str").agg(sum($"loss").as("loss_cnt"))
      //.select($"date_str",from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),$"loc_lon84",$"loc_lat84",$"icm_totalodometer",$"bcm_keyst", $"keyst",$"tripst",$"tripid")
      .sort($"date_str")
      .collect().foreach(e=>{println(e.mkString(","))})
  }

}
