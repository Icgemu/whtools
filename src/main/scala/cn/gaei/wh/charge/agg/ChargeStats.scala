package cn.gaei.wh.charge.agg

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class ChargeStats extends UserDefinedAggregateFunction {

  case class SpdInput(ts:Long, volt:Double, cur:Double, sc_temp_max:Int, sc_temp_min:Int, soc:Double)

  val InPutType = StructType(
    StructField("ts", LongType) ::
      StructField("volt", DoubleType) ::
      StructField("cur", DoubleType) ::
      StructField("sc_temp_max", IntegerType) ::
      StructField("sc_temp_min", IntegerType) ::
      StructField("soc", DoubleType) ::
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
      StructField("start_ts", LongType) ::
        StructField("end_time", LongType) ::
        StructField("charge_time", DoubleType) ::
        StructField("doc_cnt", IntegerType) ::
        StructField("soc_hops_cnt", IntegerType) ::
        StructField("soc_hops_max", DoubleType) ::
        StructField("start_soc", DoubleType) ::
        StructField("end_soc", DoubleType) ::
        StructField("volt_max", DoubleType) ::
        StructField("volt_min", DoubleType) ::
        StructField("volt_avg", DoubleType) ::
        StructField("volt_percentile", Percentile) ::
        StructField("cur_max", DoubleType) ::
        StructField("cur_min", DoubleType) ::
        StructField("cur_avg", DoubleType) ::
        StructField("cur_percentile", Percentile) ::
        StructField("sc_temp_max", IntegerType) ::
        StructField("sc_temp_min", IntegerType) ::

        StructField("charge_capacity", DoubleType) ::
        StructField("charge_energy", DoubleType) ::
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

      val sc_temp_max = if(e.isNullAt(3)) Int.MinValue else e.getInt(3)
      val sc_temp_min = if(e.isNullAt(4)) Int.MinValue else e.getInt(4)
      val soc = if(e.isNullAt(5)) Double.MinValue else e.getDouble(5)

      SpdInput(ts, volt, cur,sc_temp_max, sc_temp_min, soc)
    }) .sortWith((e1,e2) => {
      e1.ts < e2.ts
    })

    val doc_cnt = data.size

    val st = data.head.ts
    val et = data.last.ts
    val h = (et - st)/(60*60*1000.0)

    val start_soc = data.head.soc
    val end_soc = data.last.soc

    val volt_data = data.filter( _.volt > Double.MinValue ).map(_.volt).sorted
    val volt_max = volt_data.max
    val volt_min = volt_data.min
    val volt_avg = volt_data.sum / volt_data.size

    val cur_data = data.filter( _.cur > Double.MinValue ).map(_.cur)
    val cur_max = cur_data.max
    val cur_min = cur_data.min
    val cur_avg = cur_data.sum / cur_data.size

    val soc_data = data.filter( _.soc > Double.MinValue ).map(_.soc)
    val tails = soc_data.tail
    val heads = soc_data.drop(1)
    val hops = heads.zip(tails).map(e => {
      e._2 - e._1
    })
    val hops_cnt = hops.filter(_ > 0.1).size
    val hops_max = if(hops.size >0 ) hops.max else 0.0

    val sc_temp_max = data.map(_.sc_temp_max).filter(_ > Int.MinValue).max
    val sc_temp_min = data.map(_.sc_temp_min).filter(_ > Int.MinValue).min

    val W = (cur_avg * volt_avg) * (et -st) / 1000 / (3600 * 1000)
    val ah = (cur_avg) * (et -st) / (1000 * 3600)

    def fun(e:Seq[Double], p:Double):Int = (e.size * p).toInt

    val volt_index = Array(fun(volt_data ,0.01), fun(volt_data ,0.25),
      fun(volt_data ,0.5),fun(volt_data ,0.75),fun(volt_data ,0.99))
    val cur_index = Array(fun(cur_data ,0.01), fun(cur_data ,0.25),
      fun(volt_data ,0.5),fun(cur_data ,0.75),fun(cur_data ,0.99))
    val volt_val = if (volt_data.size > 0) volt_index.map( volt_data(_) ) else Array.fill(5)(0.0)
    val cur_val = if (cur_data.size > 0) cur_index.map( cur_data(_) ) else Array.fill(5)(0.0)

    Row(st,et, h,doc_cnt,hops_cnt,hops_max, start_soc,end_soc,
      volt_max, volt_min, volt_avg, Row.fromSeq(volt_val),
      cur_max, cur_min, cur_avg,Row.fromSeq(cur_val),
      sc_temp_max, sc_temp_min,
      ah, W)
  }
}
