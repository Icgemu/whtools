package cn.gaei.wh.od.agg

import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CommStats extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    StructField("ts", LongType) ::
      StructField("loc_city_name", StringType) ::
      StructField("loc_city_id", StringType) ::
      StructField("loc_lon84", DoubleType) ::
      StructField("loc_lat84", DoubleType) ::
      StructField("soc", DoubleType) ::
      Nil)

  override def bufferSchema: StructType = {
    val Location = StructType(
      StructField("lon", DoubleType) ::
        StructField("lat", DoubleType) ::
        Nil)
    StructType(
      StructField("start_time_in_ms", LongType) ::
        StructField("end_time_in_ms", LongType) ::
        StructField("start_time_in_hour", IntegerType) ::
        StructField("end_time_in_hour", IntegerType) ::
        StructField("soc_max", DoubleType) ::
        StructField("soc_min", DoubleType) ::
        StructField("start_city_name", StringType) ::
        StructField("start_city_id", StringType) ::
        StructField("start_city_location", Location) ::
        StructField("end_city_name", StringType) ::
        StructField("end_city_id", StringType) ::
        StructField("end_city_location", Location) ::
        Nil)
  }


  override def dataType: DataType = bufferSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Long.MaxValue
    buffer(1) = Long.MinValue

    buffer(2) = Int.MaxValue
    buffer(3) = Int.MinValue

    buffer(4) = Double.MinValue
    buffer(5) = Double.MaxValue

    buffer(6) = ""
    buffer(7) = ""
    buffer(8) = Row()

    buffer(9) = ""
    buffer(10) = ""
    buffer(11) = Row()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val ts = input.getLong(0)
    val t1 = buffer.getLong(0)
    val t2 = buffer.getLong(1)

    val soc = if (input.isNullAt(5)) -1.0 else input.getDouble(5)
    val soc1 = buffer.getDouble(4)
    val soc2 = buffer.getDouble(5)

    if (ts < t1) {
      buffer(0) = ts
      val h = new Date(ts).getHours()
      buffer(2) = h

      buffer(6) = input.getString(1)
      buffer(7) = input.getString(2)
      buffer(8) = Row(input.getDouble(3),input.getDouble(4))
    }
    if (ts > t2) {
      buffer(1) = ts
      val h = new Date(ts).getHours()
      buffer(3) = h

      buffer(9) = input.getString(1)
      buffer(10) = input.getString(2)
      buffer(11) = Row(input.getDouble(3),input.getDouble(4))
    }

    if (soc > 0 && soc1 < soc){
      buffer(4) = soc
    }
    if (soc > 0 && soc2 > soc){
      buffer(5) = soc
    }
  }

  override def merge(b1: MutableAggregationBuffer, b2: Row): Unit = {
    if(b1.getLong(0) > b2.getLong(0)){
      b1(0) = b2.getLong(0)
      b1(2) = b2.getInt(2)

      b1(6) = b2.getString(6)
      b1(7) = b2.getString(7)
      b1(8) = b2.get(8)
    }

    if(b1.getLong(1) < b2.getLong(1)){
      b1(1) = b2.getLong(1)
      b1(3) = b2.getInt(3)

      b1(9) = b2.getString(9)
      b1(10) = b2.getString(10)
      b1(11) = b2.get(11)
    }

    if(b1.getDouble(4) < b2.getDouble(4)){
      b1(4) = b2(4)
    }

    if(b1.getDouble(5) > b2.getDouble(5)){
      b1(5) = b2(5)
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer
  }
}
