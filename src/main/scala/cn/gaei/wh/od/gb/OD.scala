package cn.gaei.wh.od.gb

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object OD {
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
    val data = sc.read.parquet("/data/gb/parquet/*")

    import sc.implicits._
    import cn.gaei.wh._

    val out = new PrintWriter("./LMGHP1S81H1000113.csv")

    data.filter($"vin".equalTo("LMGHP1S81H1000113"))
      .filter($"d".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271 && $"veh_odo"> 0)
      .withColumn("keyst", when($"veh_st".equalTo(1), 1).otherwise(0))
      .od("tripId",$"vin",$"ts",$"keyst",$"veh_odo")
//      .setLocation("city_id","city_name",$"loc_lon84",$"loc_lat84")
      .select($"d",from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),$"loc_lon84",$"loc_lat84",$"veh_odo",$"veh_st", $"keyst",$"tripId")
      .sort($"ts")
        .selectExpr()
      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
    out.close()

  }

}
