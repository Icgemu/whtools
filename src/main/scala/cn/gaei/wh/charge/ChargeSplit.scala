package cn.gaei.wh.charge

import cn.gaei.wh.od.TripUtils.Segment
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

object ChargeSplit extends Serializable {

  private[this] def __is_stop_segment(t1:Segment ,t2:Segment):Boolean = {
    val ts_diff = t2.ts - t1.ts
    val soc_diff = t2.odo - t1.odo
    val is_stop = (ts_diff > 30 * 60 * 1000) && soc_diff < 0 // 30 minutes and soc_diff < 0
    is_stop
  }

  private[this] def __is_stop(t1:Segment ,t2:Segment):Boolean = {
    //val st = t1.st
    var res = true
    if (!__is_stop_segment(t1, t2)){
      res = false
    }
    res
  }

  def  setChargeId[T : ClassTag](ds: Dataset[T],id: String,
                                 vin:Column,ts:Column,chargeSt:Column,soc:Column): DataFrame =  {

    val sc= ds.sparkSession
    sc.udf.register("_genChargeSt", (e: Seq[Row]) => {
      val arr = e.map(e => {Segment(e.getString(0), e.getLong(1),e.getInt(2), e.getDouble(3))})
      val res = arr.size match {
        case 2 => { // window
          if(__is_stop(arr(0), arr(1))) 1 else 0 // charge start
        }
        case _ =>{
          1 //charge start
        }
      }
      res
    })

    import sc.implicits._
    val ws1 = Window.partitionBy(vin).orderBy(ts).rowsBetween(-1, Window.currentRow)
    val ws2 = Window.partitionBy(vin).orderBy(ts).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    //    val ws3 = Window.partitionBy(vin).orderBy(ts).rowsBetween(Window.currentRow, 1)
    val dff = ds.withColumn("_genChargeSt", callUDF("_genChargeSt", collect_list(struct(vin, ts, chargeSt,soc)).over(ws1)))
      //.filter($"_genChargeSt" > 0)
      //.withColumn("__flg", when($"_genStopSt".equalTo(1), 1).otherwise(0))
      .withColumn(id, sum($"_genChargeSt").over(ws2))
    //.withColumn(id, callUDF("_genId", collect_list(struct($"__id",$"_genStopSt")).over(ws3)))
    //dff.drop($"_genSt")
    dff
  }


}
