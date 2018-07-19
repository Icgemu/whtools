package cn.gaei

import cn.gaei.wh.charge.ChargeSplit
import cn.gaei.wh.od.Trip
import cn.gaei.wh.stop.StopChecker
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.elasticsearch.spark.rdd.EsSpark

import scala.reflect.ClassTag

package object wh {

  implicit def whDatasetFunctions[T : ClassTag](ds: Dataset[T]) = new WhDatasetFunctions(ds)

  class WhDatasetFunctions[T : ClassTag](ds: Dataset[T]) extends Serializable {

    def setLocation(locationIdName: String,LocationCityName:String, lat:Column,lon:Column): DataFrame =  {
      Location.setLocation(ds, locationIdName, LocationCityName, lat, lon)
    }

    def od(idName: String,vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {
      Trip.od(ds, idName, vin, ts, keyst, odo)
    }

    def markLoss(idName: String,vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {
      Trip.markLoss(ds, idName, vin, ts, keyst, odo)
    }

    def setStop(idName: String,vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {
      StopChecker.setStop(ds, idName, vin, ts, keyst, odo)
    }
    def setChargeId(idName: String,vin:Column,ts:Column,chargest:Column,soc:Column): DataFrame =  {
      ChargeSplit.setChargeId(ds, idName, vin, ts, chargest, soc)
    }

    def saveES(resource: String, cfg: scala.collection.Map[String, String]): Unit ={
      EsSpark.saveToEs(ds.rdd, resource, cfg)
    }

  }
}
