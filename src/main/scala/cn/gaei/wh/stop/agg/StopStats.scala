package cn.gaei.wh.stop.agg

import ch.hsr.geohash.WGS84Point
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class StopStats extends UserDefinedAggregateFunction{
  case class Input(ts:Long, lonlat:WGS84Point, city_id:String, city_name:String)
  override def inputSchema: StructType = StructType(
    StructField("ts", LongType) ::
      StructField("loc_lon84", DoubleType) ::
      StructField("loc_lat84", DoubleType) ::
      StructField("city_id", StringType) ::
      StructField("city_name", StringType) ::
      Nil)

  override def bufferSchema: StructType = {
    StructType(
      StructField("data", DataTypes.createArrayType(inputSchema)) ::
        Nil)
  }

  override def dataType: DataType = StructType(
    StructField("start_time", LongType) ::
      StructField("end_time", LongType) ::
      StructField("stop_time_in_second", LongType) ::
      StructField("loc_lon84", DoubleType) ::
      StructField("loc_lat84", DoubleType) ::
      StructField("city_id", StringType) ::
      StructField("city_name", StringType) ::
      Nil)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //if ( !input.isNullAt(1) && !input.isNullAt(2) ) {//longitude /latitude is not null
    val arr = buffer.getSeq[Row](0) :+ (input)
      buffer(0) = arr
    //}
  }

  override def merge(b1: MutableAggregationBuffer, b2: Row): Unit = {
    val arr = b1.getSeq[Row](0) ++ b2.getSeq[Row](0)
    b1(0) = arr
  }

  override def evaluate(buffer: Row): Any = {
    val data = buffer.getSeq[Row](0)
      .map(e =>{
      Input(e.getLong(0), new WGS84Point(e.getDouble(2), e.getDouble(1)), e.getString(3),e.getString(4))
    }) .sortWith((e1,e2) => {
      e1.ts < e2.ts
    })

    val lon = data.map(_.lonlat.getLongitude).sum / data.size
    val lat = data.map(_.lonlat.getLatitude).sum / data.size

    val duration = (data.last.ts - data.head.ts)/1000
    val st = data.head.ts
    val et = data.last.ts

    val city_id = data.head.city_id
    val city_name = data.head.city_name

    Row(st,et,duration,lon,lat,city_id,city_name)
  }
}
