package cn.gaei.wh.od

import TripUtils.{KeySt, LossSt, Segment}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{callUDF, collect_list, struct}

import scala.reflect.ClassTag

object Trip extends Serializable{

  private[this]  def _genSt(e: Seq[KeySt]): Int = {
    val res = e.size match {
      case 1 => {
        e(0).st match {
          case 1 => 2
          case _ => 0
        }
      }
      case 2 => {
        TripUtils.getODSt(e(0), e(1))
      }
    }
    res
  }

  private[this]  def _genTripID(e: Seq[Segment]): Int = {

    val res = e.size match {
      case 1 => -1
      case 2 => {
        TripUtils.getTripUuid(e(0), e(1), true)
      }
    }
    res
  }

  private[this]  def _genTripLoss(e: Seq[Segment]): Int = {

    val res = e.size match {
      case 1 => -1
      case 2 => {
        TripUtils.getTripUuid(e(0), e(1), false)
      }
    }
    res
  }

  private[this] def _lossSt(e: Seq[Row]): LossSt = {

    var res = LossSt(0,0,0)
    if (e.size == 2) {
      val t1 = e(0).getLong(0)
      val t2 = e(1).getLong(0)

      val o1 = e(0).getDouble(1)
      val o2 = e(1).getDouble(1)

      val f1 = e(0).getInt(2)
      val f2 = e(1).getInt(2)

      val s1 = e(0).getInt(3)
      val s2 = e(1).getInt(3)

      if (f2 != f1) {
        val avg_speed = ((o2 - o1 ) * 3600.0)/(((t2 - t1) / 1000.0))
        val is_data_loss = (s1 == 2 && s2 == 1 && avg_speed > 10)
        if (is_data_loss) {
          res = LossSt(1, (t2 - t1) / 1000, o2 - o1)
        }
      }
    }
    res
  }


  def od[T : ClassTag](ds: Dataset[T],id: String,
                       vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {
    val sc = ds.sparkSession

    sc.udf.register("_genSt", (e: Seq[Row]) => {
      val arr = e.map(e => {KeySt(e.getLong(0), e.getInt(1))})
      _genSt(arr)
    })

    sc.udf.register("_genTripID", (e: Seq[Row]) => {
      val arr = e.map(e => {Segment(e.getString(0), e.getLong(1),e.getInt(2),e.getDouble(3))})
      _genTripID(arr)
    })

    import sc.implicits._
    val ws1 = Window.partitionBy(vin).orderBy(ts).rowsBetween(-1, 0)
    val dff = ds.withColumn("_genSt", callUDF("_genSt", collect_list(struct(ts, keyst)).over(ws1)))
      .withColumn(id, callUDF("_genTripID", collect_list(struct(vin,ts, $"_genSt",odo)).over(ws1)))

    dff.drop($"_genSt")
  }

  def markLoss[T : ClassTag](ds: Dataset[T],id: String,vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {
    val sc = ds.sparkSession

    sc.udf.register("_genSt", (e: Seq[Row]) => {
      val arr = e.map(e => {KeySt(e.getLong(0), e.getInt(1))})
      _genSt(arr)
    })

    sc.udf.register("_genTripLoss", (e: Seq[Row]) => {
      val arr = e.map(e => {Segment(e.getString(0), e.getLong(1),e.getInt(2),e.getDouble(3))})
      _genTripLoss(arr)
    })

    sc.udf.register("_lossSt", (e: Seq[Row]) => {
      _lossSt(e)
    })

    import sc.implicits._
    val ws1 = Window.partitionBy(vin).orderBy(ts).rowsBetween(-1, 0)
    val dff = ds.withColumn("_genSt", callUDF("_genSt", collect_list(struct(ts, keyst)).over(ws1)))
      .withColumn("tripId", callUDF("_genTripLoss", collect_list(struct(vin,ts, $"_genSt",odo)).over(ws1)))
      .withColumn(id, callUDF("_lossSt", collect_list(struct(ts, odo, $"tripId", $"_genSt")).over(ws1)))
    dff.drop("_genSt")
  }

}
