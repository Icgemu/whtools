package cn.gaei.wh.charge.ag

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object Charger {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()

    val data = sc.read.parquet("/data/AG/parquet/d=20180114")

    import sc.implicits._
    import cn.gaei.wh._

    val out = new PrintWriter("./LMGGN1S56G1004602.csv")
    val cache = data
      .filter($"vin".equalTo("LMGGN1S56G1004602"))
//      .filter($"date_str".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"icm_totalodometer" > 0).count()

//      .filter($"ccs_chargerstartst".equalTo(0) && $"ccs_chargevolt".isNotNull && $"ccs_chargecur".isNotNull)
      //.withColumn("chargest", when($"ccs_chargerstartst".equalTo(0), 0).otherwise(1))
//      .setChargeId("chargeId", $"vin",$"ts",$"ccs_chargerstartst",$"icm_totalodometer".cast(DoubleType))
//      .setLocation("city_id","city_name",$"loc_lat84",$"loc_lon84")
//          .select($"date_str",
//            from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),
//            $"loc_lon84",$"loc_lat84",$"icm_totalodometer",
//            $"bcm_keyst", $"ccs_chargerstartst",$"chargeId")
//          .sort($"ts")
//    val stat = cache.groupBy($"vin",$"chargeId").agg(
//      comm_stats($"ts",$"city_id",$"city_name",$"loc_lon84",$"loc_lat84",$"bms_battsoc").as("stats"),
//      mile_stats($"ts",$"ems_engspd",$"loc_lon84",$"loc_lat84",$"icm_totalodometer").as("mile"),
//      speed_stats($"ts",$"bcs_vehspd").as("spd"),
//      bms_stats($"ts",$"bms_battvolt".cast(DoubleType),$"bms_battcurr",
//        $"bms_cellvoltmax",$"bms_cellvoltmin",
//        $"bms_batttempmax".cast(IntegerType),$"bms_batttempmin".cast(IntegerType),
//        $"ems_engspd"
//      ).as("bms")
//    )
//      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
    out.close()

  }


}
