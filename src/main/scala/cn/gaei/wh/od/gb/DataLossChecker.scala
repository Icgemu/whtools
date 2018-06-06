package cn.gaei.wh.od.gb

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession}

object DataLossChecker{

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
//    val out = new PrintWriter("./LMGAJ1S88H1000547.csv")
    val data = sc.read.parquet("/data/guobiaob/parquet/d=2018*")

    import sc.implicits._
    import cn.gaei.wh._
    data
//      .filter($"vin".equalTo("LMGAJ1S88H1000547"))
//      .filter($"d".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271 && $"veh_odo"> 0)
      .withColumn("keyst", when($"veh_st".equalTo(1), 1).otherwise(0))
      .markLoss("loss",$"vin",$"ts",$"keyst",$"veh_odo")
      .withColumn("date_str",from_unixtime($"ts"/1000,"yyyy-MM-dd"))
//      .select($"date_str",from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),
//        $"loc_lon84",$"loc_lat84",$"veh_odo",$"veh_st", $"keyst",$"tripId",$"loss.st",$"loss.ts",$"loss.odo")
//      .sort($"ts")
//      .explain(true)
//      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
      .groupBy($"date_str").agg(sum($"loss.st").as("loss_cnt"))
      .sort($"date_str")
      .collect().foreach(e=>{println(e.mkString(","))})


//    out.close()
//    print(map)
  }

}
