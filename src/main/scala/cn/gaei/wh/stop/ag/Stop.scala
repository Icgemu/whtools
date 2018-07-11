package cn.gaei.wh.stop.ag

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object Stop {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()

    val data = sc.read.parquet("/data/AG/parquet/d=2018*")

    import sc.implicits._
    import cn.gaei.wh._
    import cn.gaei.wh.stop.functions._

    val out = new PrintWriter("./LMGFJ1S53H1S00098.csv")
    val cache = data
//      .filter($"vin".equalTo("LMGFJ1S53H1S00098"))
      //.filter($"date_str".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"icm_totalodometer" > 0)
      .filter($"bcm_keyst".equalTo(2))
      .filter($"ccs_chargerstartst".equalTo(1) || $"ccs_chargerstartst".isNull)
      .withColumn("keyst", when(($"bcm_keyst".isNull) || $"bcm_keyst".equalTo(0), 0).otherwise(1))
      .setStop("stopId", $"vin",$"ts",$"keyst",$"icm_totalodometer".cast(DoubleType))
      .setLocation("city_id","city_name",$"loc_lat84",$"loc_lon84")
//      .select($"date_str",
//            from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),
//            $"loc_lon84",$"loc_lat84",$"icm_totalodometer",
//            $"bcm_keyst", $"keyst",$"ccs_chargerstartst",$"_genStopSt",$"stopId")
//          .sort($"ts")
    val stat = cache.groupBy($"vin",$"stopId").agg(
      stop_stats($"ts",$"loc_lon84",$"loc_lat84",$"city_id",$"city_name").as("stats")
    )
      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
    out.close()
  }

}
