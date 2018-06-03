package cn.gaei.wh

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import scala.reflect.ClassTag

package object od {

  implicit def sparkDatasetFunctions[T : ClassTag](ds: Dataset[T]) = new TripDatasetFunctions(ds)

  class TripDatasetFunctions[T : ClassTag](ds: Dataset[T]) extends Serializable {

    def od(idName: String,vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {
      Trip.od(ds, idName, vin, ts, keyst, odo)
    }

    def markLoss(idName: String,vin:Column,ts:Column,keyst:Column,odo:Column): DataFrame =  {
      Trip.markLoss(ds, idName, vin, ts, keyst, odo)
    }
  }

}
