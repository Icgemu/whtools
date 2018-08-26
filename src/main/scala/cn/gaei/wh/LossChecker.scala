package cn.gaei.wh

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object LossChecker {
  private[this] val _VinToUuid = scala.collection.mutable.Map[String, Int]()
  case class Loss(vin:String, c: Int, in:Int, out:Int)
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()
    val gb = sc.read.parquet("/data/loss/")
    val out = new PrintWriter("./LHGH1181XK8000004.csv")
    import sc.implicits._
    sc.udf.register("__uid", (e:Seq[Row]) => {
      val data = e.map(d=>{Loss(d.getString(0),d.getInt(1), if(d.isNullAt(2)) -1 else d.getInt(2),if(d.isNullAt(3)) -1 else d.getInt(3))})
      val r = if(data.size ==1) data(0) else data(1)
      val uid = _VinToUuid.getOrElse(r.vin,0)
      var res = uid
      if(r.c == 1) {
        _VinToUuid += (r.vin -> (uid+1))
        res = uid +1
      }
      if(r.c == 4) {
        _VinToUuid += (r.vin -> (uid+1))
        res = uid
      }
      res
    })
    val ws1 = Window.partitionBy($"vin").orderBy($"ts",$"command_flag").rowsBetween(-1, 0)
    val fn = new LossStats2()
    gb
//      .filter($"vin".equalTo("LHGH1181XK8000004"))
      .filter($"vintype".equalTo("A5HEV"))
        .withColumn("uid",callUDF("__uid", collect_list(struct($"vin",$"command_flag",$"login_seq",$"logout_seq")).over(ws1)))
        .groupBy($"uid",reverse($"vin").as("rvin")).agg(min($"ts").as("sst"),max($"ts").as("eet"),fn($"ts",$"command_flag",$"login_seq",$"logout_seq",$"alm_lvl_higest").as("stats"))
        .select(reverse($"rvin").as("vin"),
          $"uid",
          $"sst",$"eet",
          $"stats.st".as("st"),
          $"stats.et".as("et"),
          $"stats.v_flag".as("v_flag"),
          $"stats.g_flag".as("g_flag"),
          $"stats.h_count".as("h_count"),
          $"stats.send_cnt".as("send_cnt"),
          $"stats.re_send_cnt".as("re_send_cnt"),
          $"stats.lvl_cnt".as("lvl_cnt"),
          $"stats.login_seq".as("login_seq"),
          $"stats.logout_seq".as("logout_seq"))
//      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
      .repartition(4).write.parquet("/tmp/loss1")
//      .explain()
    out.close()
  }
}
