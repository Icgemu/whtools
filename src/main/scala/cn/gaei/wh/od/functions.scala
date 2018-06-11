package cn.gaei.wh.od

import cn.gaei.wh.od.agg.{BmsStats, CommStats, MileStats, SpeedStats}
import org.apache.spark.sql.{Column, TypedColumn}

object functions {
  /**
    * aggregation function for OD time relative.
    * @param col
    * @return
    *   StructType(
          StructField("start_time_in_ms", LongType) ::
          StructField("end_time_in_ms", LongType) ::
          StructField("start_time_in_hour", IntegerType) ::
          StructField("end_time_in_hour", IntegerType) ::
          StructField("soc_max", DoubleType) ::
          StructField("soc_min", DoubleType) ::
          StructField("start_city_name", StringType) ::
          StructField("start_city_id", StringType) ::
          StructField("start_city_location", DataTypes.createArrayType(DoubleType)) ::
          StructField("end_city_name", StringType) ::
          StructField("end_city_id", StringType) ::
          StructField("end_city_location", DataTypes.createArrayType(DoubleType)) ::
        Nil)
    */
  def comm_stats(ts: Column,
                 city_name: Column,
                 city_id: Column,
                 lon84: Column,
                 lat84: Column,
                 soc:Column
                ) = {
    val fn = new CommStats()
    fn(ts, city_name, city_id, lon84, lat84, soc)
  }

  def mile_stats(ts: Column,
                 eng_spd: Column,
                 lon84: Column,
                 lat84: Column,
                 odo:Column
                ) = {
    val fn = new MileStats()
    fn(ts, eng_spd, lon84, lat84, odo)
  }

  def speed_stats(ts: Column,
                 spd: Column
                ) = {
    val fn = new SpeedStats()
    fn(ts, spd)
  }

  //ts:Long, volt:Double, cur:Double, sc_volt_max:Double,
  //sc_volt_min:Double, sc_temp_max:Int, sc_temp_min:Int, eng_spd:Int
  def bms_stats(ts: Column, volt: Column, cur: Column, sc_volt_max: Column,
                sc_volt_min: Column, sc_temp_max: Column, sc_temp_min: Column, eng_spd: Column
                 ) = {
    val fn = new BmsStats()
    fn(ts, volt,cur,sc_volt_max,sc_volt_min,sc_temp_max,sc_temp_min,eng_spd)
  }

}
