package cn.gaei.wh.stop

import cn.gaei.wh.od.agg.MileStats
import cn.gaei.wh.stop.agg.StopStats
import org.apache.spark.sql.Column

object functions {

  def stop_stats(ts: Column,
                 lon84: Column,
                 lat84: Column,
                 city_id:Column,
                 city_name:Column
                ) = {
    val fn = new StopStats()
    fn(ts, lon84, lat84, city_id, city_name)
  }

}
