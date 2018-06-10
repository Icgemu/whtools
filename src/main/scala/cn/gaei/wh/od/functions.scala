package cn.gaei.wh.od

import cn.gaei.wh.od.agg.{CommStats, MileStats}
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

}
