package cn.gaei.wh.od.gb

import java.io.PrintWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object OD {
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
    val data = sc.read.parquet("/data/gb/parquet/*")

    import sc.implicits._
    import cn.gaei.wh._
    import cn.gaei.wh.od.functions._
    val out = new PrintWriter("./LMGHP1S81H1000113.csv","UTF-8")



    val cache = data.filter($"vin".equalTo("LMGHP1S81H1000113"))
      .filter($"d".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293
        && $"loc_lat84" < 55.8271 && $"veh_odo"> 0)
      .withColumn("keyst", when($"veh_st".equalTo(1), 1).otherwise(0))
      .od("tripId",$"vin",$"ts",$"keyst",$"veh_odo")
      .setLocation("city_id","city_name",$"loc_lat84",$"loc_lon84")
//      .select(from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),
//        $"loc_lon84",$"loc_lat84",$"city_id",$"city_name",$"veh_odo",$"veh_st", $"keyst",$"tripId")
//      .sort($"ts")
      val stat = cache.groupBy($"vin",$"tripId").agg(
            comm_stats($"ts",$"city_id",$"city_name",$"loc_lon84",$"loc_lat84",$"veh_soc").as("stats"),
            mile_stats($"ts",when($"eng_spd".isNull, 0).otherwise($"eng_spd"),$"loc_lon84",$"loc_lat84",$"veh_odo").as("mile")
          )
      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
    out.close()

  }

}
