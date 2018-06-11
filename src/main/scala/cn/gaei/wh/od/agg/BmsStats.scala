package cn.gaei.wh.od.agg

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class BmsStats extends UserDefinedAggregateFunction {

  case class SpdInput(ts:Long, volt:Double, cur:Double, sc_volt_max:Double,
                      sc_volt_min:Double, sc_temp_max:Int, sc_temp_min:Int, eng_spd:Int)

  val InPutType = StructType(
    StructField("ts", LongType) ::
      StructField("volt", DoubleType) ::
      StructField("cur", DoubleType) ::
      StructField("sc_volt_max", DoubleType) ::
      StructField("sc_volt_min", DoubleType) ::
      StructField("sc_temp_max", IntegerType) ::
      StructField("sc_temp_min", IntegerType) ::
      StructField("eng_spd", IntegerType) ::
      Nil)

  override def inputSchema: StructType = InPutType

  override def bufferSchema: StructType = {
    StructType(
      StructField("data", DataTypes.createArrayType(InPutType)) ::
        Nil)
  }


  override def dataType: DataType = {
    val Percentile = StructType(
        StructField("p1", DoubleType) ::
          StructField("p25", DoubleType) ::
          StructField("p50", DoubleType) ::
          StructField("p75", DoubleType) ::
          StructField("p99", DoubleType) ::
          Nil)
    StructType(
      StructField("volt_max", DoubleType) ::
        StructField("volt_min", DoubleType) ::
        StructField("volt_avg", DoubleType) ::
        StructField("cur_max", DoubleType) ::
        StructField("cur_min", DoubleType) ::
        StructField("cur_avg", DoubleType) ::
        StructField("sc_temp_max", IntegerType) ::
        StructField("sc_temp_min", IntegerType) ::
        StructField("gm_run_time", IntegerType) ::
        StructField("gm_run_records", IntegerType) ::
        StructField("sc_volt_diff_max", DoubleType) ::
        StructField("sc_volt_diff_percentile", Percentile) ::
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
    val data = buffer.getSeq[Row](0).map(e =>{
      val ts = e.getLong(0)
      val volt = if(e.isNullAt(1)) Double.MinValue else e.getDouble(1)
      val cur = if(e.isNullAt(2)) Double.MinValue else e.getDouble(2)

      val sc_volt_max = if(e.isNullAt(3)) Double.MinValue else e.getDouble(3)
      val sc_volt_min = if(e.isNullAt(4)) Double.MinValue else e.getDouble(4)

      val sc_temp_max = if(e.isNullAt(5)) Int.MinValue else e.getInt(5)
      val sc_temp_min = if(e.isNullAt(6)) Int.MinValue else e.getInt(6)
      val eng_spd = if(e.isNullAt(7)) 0 else e.getInt(7)

      SpdInput(ts, volt, cur, sc_volt_max, sc_volt_min,sc_temp_max, sc_temp_min, eng_spd)
    }) .sortWith((e1,e2) => {
      e1.ts < e2.ts
    })

    val volt_data = data.filter( _.volt > Double.MinValue ).map(_.volt)
    val volt_max = volt_data.max
    val volt_min = volt_data.min
    val volt_avg = volt_data.sum / volt_data.size

    val cur_data = data.filter( _.cur > Double.MinValue ).map(_.cur)
    val cur_max = cur_data.max
    val cur_min = cur_data.min
    val cur_avg = cur_data.sum / cur_data.size

    val sc_temp_max = data.map(_.sc_temp_max).filter(_ > Int.MinValue).max
    val sc_temp_min = data.map(_.sc_temp_min).filter(_ > Int.MinValue).min

    val gm_run_records = data.map(_.eng_spd).filter(_ >= 0).size
    val gm_run_time = gm_run_records * 10


    val sc_volt_diff = data.filter(e=> (!e.sc_volt_max.isNaN) && (!e.sc_volt_min.isNaN))
      .map(e => e.sc_volt_max - e.sc_volt_min).sortWith((s1,s2) => {s1 < s2})
    val sc_volt_diff_max = sc_volt_diff.max

    def fun(e:Seq[Double], p:Double):Int = (e.size * p).toInt

    val volt_diff_index = Array(fun(sc_volt_diff ,0.01), fun(sc_volt_diff ,0.25),
      fun(sc_volt_diff ,0.5),fun(sc_volt_diff ,0.75),fun(sc_volt_diff ,0.99))
    val volt_diff_val = if (sc_volt_diff.size > 0) volt_diff_index.map( sc_volt_diff(_) ) else Array.fill(5)(0.0)

    Row(volt_max, volt_min, volt_avg,
      cur_max, cur_min, cur_avg,
      sc_temp_max, sc_temp_min,
      gm_run_time, gm_run_records,
      sc_volt_diff_max, Row.fromSeq(volt_diff_val))
  }
}
