package cn.gaei.wh.stop.gb

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_unixtime, when}
import org.apache.spark.sql.types.DoubleType

object Stop {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()

    val data = sc.read.parquet("/data/guobiao/parquet/d=2018*")

    import sc.implicits._
    import cn.gaei.wh._
    import cn.gaei.wh.stop.functions._

    val out = new PrintWriter("./LMGFJ1S5XH1000027.csv")
    val cache = data
//      .filter($"vin".equalTo("LMGFJ1S5XH1000027"))
//      .filter($"date_str".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"veh_odo" > 0)
      .filter($"veh_st".equalTo(1))
      //.filter($"veh_chargest".equalTo(1) || $"veh_chargest".isNull)
      .withColumn("keyst", when($"veh_st".equalTo(1), 1).otherwise(0))
      .setStop("stopId", $"vin",$"ts",$"keyst",$"veh_odo".cast(DoubleType))
      .setLocation("city_id","city_name",$"loc_lat84",$"loc_lon84")
//      .select(from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),
//        $"loc_lon84",$"loc_lat84",$"veh_odo",
//        $"veh_st", $"keyst",$"veh_chargest",$"_genStopSt",$"stopId")
//      .sort($"ts")

    val stat = cache.groupBy($"vin",$"stopId").agg(
      stop_stats($"ts",$"loc_lon84",$"loc_lat84",$"city_id",$"city_name").as("stats")
    ).collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
    out.close()

  }

}
