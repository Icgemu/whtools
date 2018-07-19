package cn.gaei.wh.charge.ag

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object Charger {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()

    val data = sc.read.parquet("/data/AG/parquet/d=2018*")

    import sc.implicits._
    import cn.gaei.wh._
    import cn.gaei.wh.charge.functions._

    val out = new PrintWriter("./LMGGN1S52G1004256.csv")
    val cache = data
      .filter($"vin".equalTo("LMGGN1S52G1004256"))
//      .filter($"date_str".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
//      .filter($"icm_totalodometer" > 0)

      .filter($"bms_battst".equalTo(12) && $"ccs_chargevolt" >0 && $"ccs_chargecur" > 0)
      //.withColumn("chargest", when($"ccs_chargerstartst".equalTo(0), 0).otherwise(1))
      .setChargeId("chargeId", $"vin",$"ts",$"ccs_chargerstartst",$"bms_battsoc")
//      .select($"date_str", from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"), $"loc_lon84",$"loc_lat84",
//            $"ccs_chargerstartst",$"ccs_chargevolt",$"ccs_chargecur", $"bms_battst", $"bms_battsoc",$"chargeId")
//          .sort($"ts")
    val stat = cache.groupBy($"vin",$"chargeId").agg(
      charge_stats($"ts",$"ccs_chargevolt".cast(DoubleType),$"ccs_chargecur",
        $"bms_batttempmax".cast(IntegerType),$"bms_batttempmin".cast(IntegerType),
        $"bms_battsoc"
      ).as("stats"), avg($"loc_lon84").as("loc_lon84"),avg($"loc_lat84").as("loc_lat84")
    )
  .setLocation("city_id","city_name",$"loc_lat84",$"loc_lon84")
      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
//    println(cache)
    out.close()

  }


}
