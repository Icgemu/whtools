package cn.gaei.wh.charge

import cn.gaei.wh.charge.agg.ChargeStats
import org.apache.spark.sql.Column

object functions {

  //ts:Long, volt:Double, cur:Double, sc_volt_max:Double,
  //sc_volt_min:Double, sc_temp_max:Int, sc_temp_min:Int, eng_spd:Int
  def charge_stats(ts: Column, volt: Column, cur: Column,
                sc_temp_max: Column, sc_temp_min: Column, soc: Column
                 ) = {
    val fn = new ChargeStats()
    fn(ts, volt,cur,sc_temp_max,sc_temp_min,soc)
  }

}
