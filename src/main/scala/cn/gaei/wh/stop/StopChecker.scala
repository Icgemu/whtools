package cn.gaei.wh.stop

import cn.gaei.wh.od.TripUtils.{KeySt, Segment}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{callUDF, collect_list, struct}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.reflect.ClassTag

object StopChecker extends Serializable{

  private[this] def __is_stop_segment(t1:Segment ,t2:Segment):Boolean = {
    val ts_diff = t2.ts - t1.ts
    val odo = t2.odo - t1.odo
    val avg_speed = ((odo ) * 3600.0)/(((ts_diff) / 1000.0))
    val is_stop = (ts_diff > 300 * 1000) && avg_speed > 10 // 5 minutes and avg_speed >10km/h
    is_stop
  }

  private[this] def __is_stop(t1:Segment ,t2:Segment):Boolean = {
    val st = t1.st
    var res = true
    if (st != 0) { // key off
      if (!__is_stop_segment(t1, t2)){
        res = false
      }
    }
    res
  }

  def  setStop[T : ClassTag](ds: Dataset[T],id: String,
    vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {

    val sc= ds.sparkSession
    sc.udf.register("_genStopSt", (e: Seq[Row]) => {
      val arr = e.map(e => {Segment(e.getString(0), e.getLong(1),e.getInt(2),e.getDouble(3))})
      val res = arr.size match {
        case 2 => { // window start/stop
          if(__is_stop(arr(0), arr(1))) 1 else 0 // stop start
        }
        case 3 =>{
          var b = if(__is_stop(arr(1), arr(2))) 1 else 0// stop start
          if(b == 0){
            b = if(__is_stop(arr(0), arr(1))) 2 else 0 // stop end
          }
          b
        }
        case _ =>{
          0
        }
      }
      res
    })

    sc.udf.register("_genStopID", (e: Seq[Row]) => {
      val arr = e.map(e => {Segment(e.getString(0), e.getLong(1),e.getInt(2),e.getDouble(3))})
      val res = arr.size match {
        case 2 => { // window start/stop
          if(__is_stop(arr(0), arr(1))) 1 else 0 // stop start
        }
        case 3 =>{
          var b = if(__is_stop(arr(1), arr(2))) 1 else 0// stop start
          if(b == 0){
            b = if(__is_stop(arr(0), arr(1))) 2 else 0 // stop end
          }
          b
        }
        case _ =>{
          0
        }
      }
      res
    })


    import sc.implicits._
    val ws1 = Window.partitionBy(vin).orderBy(ts).rowsBetween(-1, 1)
    val dff = ds.withColumn("_genStopSt", callUDF("_genStopSt", collect_list(struct(vin,ts, keyst, odo)).over(ws1)))
          .filter($"_genSt" > 0)
      .withColumn(id, callUDF("_genStopID", collect_list(struct(vin,ts, $"_genStopSt",odo)).over(ws1)))
    dff.drop($"_genSt")
  }

}
