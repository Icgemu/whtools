package cn.gaei.wh

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{when, _}

object LossChecker {


  case class LossCls(vin:String,vintype:String,ts:Long,command_flag:Int,login_seq:Int,logout_seq:Int,alm_lvl_higest:Int,uid:Int)
  case class Loss(vin:String, c: Int, in:Int, out:Int)
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().config(new SparkConf()).getOrCreate()

//    val out = new PrintWriter("./LMGFJ1S51H1000109.csv")
    import sc.implicits._
//    sc.udf.register("__uid", (e:Seq[Row]) => {
//      val data = e.map(d=>{Loss(d.getString(0),d.getInt(1), if(d.isNullAt(2)) -1 else d.getInt(2),if(d.isNullAt(3)) -1 else d.getInt(3))})
//      val r = if(data.size ==1) data(0) else data(1)
//      val uid = _VinToUuid.getOrElse(r.vin,0)
//      var res = uid
//      if(r.c == 1) {
//        _VinToUuid += (r.vin -> (uid+1))
//        res = uid +1
//      }
//      if(r.c == 4) {
//        _VinToUuid += (r.vin -> (uid+1))
//        res = uid
//      }
//      res
//    })
//    val ws1 = Window.partitionBy($"vin").orderBy($"ts",$"command_flag").rowsBetween(-1, 0)
//    val fn = new LossStats2()
    val gb = sc.read.parquet("/data/loss/")
//            .filter($"vin".equalTo("LMGFJ1S51H1000109"))
//            .filter($"vintype".equalTo("A5HEV"))
      .select($"vin",$"ts",$"vintype",$"command_flag",$"login_seq",$"logout_seq",$"alm_lvl_higest")
        .repartition($"vin").sortWithinPartitions($"vin",$"ts",$"command_flag")
        .mapPartitions(par=>{
          val _VinToUuid = scala.collection.mutable.Map[String, Int]()
          par.map(d=>{
            val r = Loss(d.getString(0),d.getInt(3), if(d.isNullAt(4)) -1 else d.getInt(4),if(d.isNullAt(5)) -1 else d.getInt(5))
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
            LossCls(d.getString(0),d.getString(2),d.getLong(1),r.c,r.in,r.out,if(d.isNullAt(6)) 0 else d.getInt(6),res)
          })
        })
//        .withColumn("uid",callUDF("__uid", collect_list(struct($"vin",$"command_flag",$"login_seq",$"logout_seq")).over(ws1)))
        .write.parquet("/tmp/check1")

    sc.read.parquet("/tmp/check1/")
        .withColumn("is_login",when($"command_flag".equalTo(1),1).otherwise(0))
      .withColumn("is_logout",when($"command_flag".equalTo(4),1).otherwise(0))
        .withColumn("is_h",when($"command_flag".equalTo(7),1).otherwise(0))
        .withColumn("is_send",when($"command_flag".equalTo(2),1).otherwise(0))
        .withColumn("is_re",when($"command_flag".equalTo(3),1).otherwise(0))
      .withColumn("is_lvl",when($"alm_lvl_higest".equalTo(3),1).otherwise(0))
        .withColumn("login_id",when($"login_seq".isNull,-1).otherwise($"login_seq"))
        .withColumn("logout_id",when($"logout_seq".isNull,-1).otherwise($"logout_seq"))
        .write.parquet("/tmp/check2")

        //.groupBy($"vin",$"vintype",$"uid").agg(min($"ts").as("sst"),max($"ts").as("eet"),fn($"ts",$"command_flag",$"login_seq",$"logout_seq",$"alm_lvl_higest").as("stats"))
    sc.read.parquet("/tmp/check2/").groupBy($"vin",$"vintype",$"uid").agg(
        min($"ts").as("st"),
        max($"ts").as("et"),
        sum($"is_h").as("h_count"),
        sum($"is_send").as("send_cnt"),
        sum($"is_re").as("re_send_cnt"),
        sum($"is_lvl").as("lvl_cnt"),
        sum($"is_login").as("login_cnt"),
        sum($"is_logout").as("logout_cnt"),
        max($"login_id").as("login_seq"),
        max($"logout_id").as("logout_seq")
      ).write.parquet("/tmp/check3")


    sc.read.parquet("/tmp/check3/").withColumn("v_flag",when($"login_cnt" > 0 && $"logout_cnt" > 0 && $"login_seq".equalTo($"logout_seq") , 0).otherwise(1))
      .withColumn("g_flag",
        when($"login_cnt" > 0 && $"logout_cnt" > 0, 0)
            .when($"login_cnt" > 0 && $"logout_cnt".equalTo(0) , 1)
          .when($"login_cnt".equalTo(0) && $"logout_cnt" > 0, 2)
          .otherwise(3))
      .withColumn("respect",(($"et"- $"st")/10000))
        .withColumn("response",($"send_cnt" + $"re_send_cnt"))
      .select($"vin",$"vintype",
          $"uid",$"st",$"et",
          when($"login_cnt".equalTo(0),-1).otherwise($"st").as("sst"),
          when($"logout_cnt".equalTo(0),-1).otherwise($"et").as("eet"),
          $"v_flag",
          $"g_flag",
          $"h_count",
          $"send_cnt",
          $"re_send_cnt",
          $"lvl_cnt",
          $"login_seq",
          $"logout_seq",
          $"response",
          $"respect",
         (lit(1)-$"response"/$"respect").as("loss")
      )
//      .collect().foreach(e=>{out.write(e.mkString(",")+"\n")})
      .repartition(4).write.parquet("/tmp/check4")
//      .explain()
//    out.close()
  }
}
