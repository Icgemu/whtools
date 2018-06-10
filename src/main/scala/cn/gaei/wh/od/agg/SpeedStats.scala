package cn.gaei.wh.od.agg

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class SpeedStats extends UserDefinedAggregateFunction {

  case class Input(ts:Long, spd:Integer)
  val InPutType = StructType(
    StructField("ts", LongType) ::
      StructField("spd", IntegerType) ::
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
    val SPD_Distribution = StructType(
      StructField("d_0", IntegerType) ::
        StructField("d_10", IntegerType) ::
        StructField("d_20", IntegerType) ::
        StructField("d_30", IntegerType) ::
        StructField("d_40", IntegerType) ::
        StructField("d_50", IntegerType) ::
        StructField("d_60", IntegerType) ::
        StructField("d_70", IntegerType) ::
        StructField("d_80", IntegerType) ::
        StructField("d_90", IntegerType) ::
        StructField("d_100", IntegerType) ::
        StructField("d_110", IntegerType) ::
        StructField("d_120", IntegerType) ::
        StructField("d_120_up", IntegerType) ::
        Nil)
    StructType(
      StructField("spd_max_in_kmh", DoubleType) ::
        StructField("spd_min_in_kmh", DoubleType) ::
        StructField("spd_avg_in_kmh", DoubleType) ::
        StructField("spd_run_avg_in_kmh", DoubleType) ::
        StructField("time_idle_in_min", DoubleType) ::
        StructField("idle_record_cnt", DoubleType) ::
        StructField("acc_spd_percentile", Percentile) ::
        StructField("dec_spd_percentile", Percentile) ::
        StructField("spd_distribution", SPD_Distribution) ::
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
      Input(e.getLong(0),e.getInt(1))
    }) .sortWith((e1,e2) => {
      e1.ts < e2.ts
    })

    val spd_max = data.map(_.spd).max
    val spd_min = data.map(_.spd).min
    val spd_avg = data.map(_.spd).sum / data.size
    val run_data = data.map(_.spd).filter(_ >= 5.0)
    val idle_data = data.map(_.spd).filter(_ < 5.0)
    val spd_run_avg = run_data.sum / run_data.size

    val idle_time = (idle_data.size * 10 ) / 60.0
    val idle_cnt = idle_data.size
    val spd_dist = Array.fill(14)(0)
    data.map( _.spd.toInt).foreach(spd => {
      val s = spd % 10
      var index = (spd - s) / 10
      if(index > 12) {index = 13}
      spd_dist(index) = spd_dist(index) + 1
    })


    val par0 = data.take(data.size-1)
    val par1 = data.tail

    val acc_all = par0.zip(par1).map(e =>{
      val (last, cur) = e
      val spd_diff = cur.spd - last.spd
      val time_diff = cur.ts - last.ts
      (spd_diff *1000.0) / (time_diff/1000.0)
    })

    val acc = acc_all.filter( _ > 0 ).sortWith((s1,s2) => {s1 < s2})
    val dec = acc_all.filter( _ < 0 ).sortWith((s1,s2) => {s1 > s2})

    def fun(e:Seq[Double], p:Double):Int = (e.size * p).toInt

    val acc_index = Array(fun(acc ,0.01), fun(acc ,0.25),fun(acc ,0.5),fun(acc ,0.75),fun(acc ,0.99))
    val dec_index = Array(fun(dec ,0.01), fun(dec ,0.25),fun(dec ,0.5),fun(dec ,0.75),fun(dec ,0.99))
    val acc_val = acc_index.map( acc(_) )
    val dec_val = dec_index.map( dec(_) )
    Row(spd_max, spd_min, spd_avg, spd_run_avg, idle_time, idle_cnt,
      Row.fromSeq(acc_val), Row.fromSeq(dec_val),Row.fromSeq(spd_dist))
  }
}
