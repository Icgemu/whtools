package cn.gaei.wh.od.agg

import java.util.Date

import ch.hsr.geohash.{GeoHash, WGS84Point}
import ch.hsr.geohash.util.VincentyGeodesy
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MileStats extends UserDefinedAggregateFunction {

  case class Input(ts:Long, eng_spd:Integer, lonlat:WGS84Point, odo:Double)
  val InPutType = StructType(
    StructField("ts", LongType) ::
      StructField("eng_spd", IntegerType) ::
      StructField("loc_lon84", DoubleType) ::
      StructField("loc_lat84", DoubleType) ::
      StructField("odo", DoubleType) ::
      Nil)

  override def inputSchema: StructType = InPutType

  override def bufferSchema: StructType = {
    StructType(
      StructField("data", DataTypes.createArrayType(InPutType)) ::
        Nil)
  }


  override def dataType: DataType = {
    StructType(
      StructField("mileage_on_odometer_in_km", DoubleType) ::
        StructField("mileage_on_gps_in_km", DoubleType) ::
        StructField("mileage_on_battery_in_km", DoubleType) ::
        StructField("mileage_on_fuel_in_km", DoubleType) ::
        StructField("time_in_min", DoubleType) ::
        StructField("time_on_battery_in_min", DoubleType) ::
        StructField("time_on_fuel_in_min", DoubleType) ::
        Nil)
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val arr = buffer.getSeq[Row](0) :+ (input)
    buffer(0) = arr
  }

  override def merge(b1: MutableAggregationBuffer, b2: Row): Unit = {
    val arr = b1.getSeq[Row](0) ++ b2.getSeq[Row](0)
    b1(0) = arr
  }

  override def evaluate(buffer: Row): Any = {
    val data = buffer.getSeq[Row](0).toList.map(e =>{

      Input(e.getLong(0),e.getInt(1), new WGS84Point(e.getDouble(3), e.getDouble(2)), e.getDouble(4))
    }) .sortWith((e1,e2) => {
      e1.ts < e2.ts
    })

    val dist_odo = data.last.odo - data.head.odo
    val times = (data.last.ts - data.head.ts)/(1000.0*60.0)
    val par0 = data.take(data.size-1)
    val par1 = data.tail

    var dist_gps = 0d
    var elec_dist_gps = 0d
    var fuel_dist_gps = 0d

    var elec_time_gps = 0d
    var fuel_time_gps = 0d

    par0.zip(par1).map(e =>{
      val (last,cur) = e
      val dist = VincentyGeodesy.distanceInMeters(last.lonlat, cur.lonlat)/1000.0
      dist_gps += dist
      val time_diff = (cur.ts - last.ts)/(1000.0*60)
      val is_elec = last.eng_spd == 0 && cur.eng_spd == 0
      val is_fuel = last.eng_spd != 0 && cur.eng_spd != 0

      if (is_elec){
        elec_dist_gps += dist
        elec_time_gps += time_diff
      }else{
        if (is_fuel) {
          fuel_dist_gps += dist
          fuel_time_gps += time_diff
        }else{
          fuel_dist_gps += dist/2.0
          elec_dist_gps += dist/2.0
          fuel_time_gps += time_diff/2.0
          elec_time_gps += time_diff/2.0
        }
      }
    })
    Row(dist_odo, dist_gps, fuel_dist_gps, elec_dist_gps, times, elec_time_gps, fuel_time_gps)
  }
}
