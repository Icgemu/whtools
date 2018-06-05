package cn.gaei.wh.od.ag

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object DataLossChecker {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()

    val data = sc.read.parquet("/data/ag/parquet/*")

    import sc.implicits._
    import cn.gaei.wh._
    val out = new PrintWriter("./LMGGN1S55F1000510.csv")

    data
      .filter($"vin".equalTo("LMGGN1S55F1000510"))
      .filter($"date_str".startsWith("2018051"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"icm_totalodometer" > 0)
      .withColumn("keyst", when(($"bcm_keyst".isNull) || $"bcm_keyst".equalTo(0), 0).otherwise(1))
      .markLoss("loss",$"vin", $"ts", $"keyst", $"icm_totalodometer".cast(DoubleType))
//      .groupBy($"date_str").agg(sum($"loss").as("loss_cnt"))
      .select($"date_str",
        from_unixtime($"ts"/1000,"yyyy-MM-dd HH:mm:ss"),
        $"loc_lon84",$"loc_lat84",$"icm_totalodometer",$"bcm_keyst", $"keyst",$"loss")
      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
//      .sort($"date_str")
//      .collect().foreach(e=>{println(e.mkString(","))})
    out.close()
  }

}
