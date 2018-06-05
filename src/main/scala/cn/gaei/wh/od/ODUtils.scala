package cn.gaei.wh.od

object ODUtils extends Serializable{

  case class Segment(vin:String,ts:Long, st:Int,odo:Double)
  case class KeySt(ts:Long, st:Int)
  case class LossSt(st:Int, ts:Long, odo:Double)

  private[this] val _VinToUuid = scala.collection.mutable.Map[String, (Long,Int)]()

  def getODSt(last:KeySt, cur:KeySt):Int = {
    val t1 = last.ts
    val t2 = cur.ts

    val s1 =last.st
    val s2 = cur.st

    val tolerance = t2 - t1 < 300 * 1000
    val is_start = (s1 != 1) && (s2 == 1)
    val is_mid = (s1 == 1) && (s2 == 1)
    val is_end = (s1 == 1) && (s2 != 1)
    var res = 0
    if (tolerance && is_start) {
      res = 1//OD start
    } else if (tolerance && is_mid) {
      res = 2//OD
    } else if (tolerance && is_end) {
      res = 3//OD stop
    } else if (!tolerance) {
      if (s2 == 1) {
        res = 1
      } else {
        //res = 3
        //TODO:check odo to set state
      }
    }
    res
  }


  def getTripUuid(last:Segment, cur:Segment, mergeLoss:Boolean):Int = {
    val vin = last.vin

    val t1 = last.ts
    val t2 = cur.ts

    val s1 = last.st
    val s2 = cur.st

    val o1 = last.odo
    val o2 = cur.odo
    var res = -1

    if (s2 == 1) {//od start,new uuid
      val (ts, cnt) = _VinToUuid.getOrElse(vin, (0,0))
      //check data loss
      val avg_speed = ((o2 - o1 ) *  3600.0) /((t2 - t1) / 1000.0)
      val is_loss = if(s1 == 2 && avg_speed > 10 && mergeLoss) true else false

      if ((ts != t2) && !is_loss) {
        res = cnt + 1 //not data loss, uuid add one
      } else {
        res = cnt //not data loss or merge loss;
      }
      _VinToUuid += (vin -> (t2, res))
    }

    if (s2 == 2) res = _VinToUuid.getOrElse(vin, (0,0))._2
    if (s1 == 2 && s2 == 3) res = _VinToUuid.getOrElse(vin, (0,0))._2
    res
  }

}
