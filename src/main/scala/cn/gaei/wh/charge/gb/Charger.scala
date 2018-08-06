package cn.gaei.wh.charge.gb

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object Charger {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()

    val data = sc.read.parquet("/data/guobiao/parquet/d=2018*")

    import cn.gaei.wh._
    import cn.gaei.wh.charge.functions._
    import sc.implicits._

    val out = new PrintWriter("./LMGHP1S81H1000113.csv")
    val cache = data
//      .filter($"vin".equalTo("LMGHP1S81H1000113"))
//      .filter($"date_str".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"veh_chargest".equalTo(1) && $"esd_volt" >0)
      .setChargeId("chargeId", $"vin",$"ts",$"veh_soc".cast(DoubleType))
//      .select($"ts", from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"), $"loc_lon84",$"loc_lat84",$"esd_volt",$"esd_curr", $"veh_st", $"veh_soc",$"veh_chargest")
//      .select($"ts", from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"), $"loc_lon84",$"loc_lat84",
//            $"veh_chargest",$"esd_volt",$"esd_curr", $"veh_st", $"veh_soc",$"chargeId")
//          .sort($"ts")
    val stat = cache.groupBy($"vin",$"chargeId").agg(
      charge_stats($"ts",$"esd_volt".cast(DoubleType),abs($"esd_curr"),
        $"data_batt_temp_highest".cast(IntegerType),$"data_batt_temp_lowestest".cast(IntegerType),
        $"veh_soc".cast(DoubleType)
      ).as("stats"), avg($"loc_lon84").as("loc_lon84"),avg($"loc_lat84").as("loc_lat84")
    )
  .setLocation("city_id","city_name",$"loc_lat84",$"loc_lon84")
//  .filter($"stats.soc_hops_cnt" > 0)
      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
//    println(cache)
    out.close()
  }

}
