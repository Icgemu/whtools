package cn.gaei.wh

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class LossStats2 extends UserDefinedAggregateFunction {

  case class SpdInput(ts:Long, command_flag:Int, login_seq:Int, logout_seq:Int,
                      lvl:Int)

  val InPutType = StructType(
    StructField("ts", LongType) ::
      StructField("command_flag", IntegerType) ::
      StructField("login_seq", IntegerType) ::
      StructField("logout_seq", IntegerType) ::
      StructField("lvl", IntegerType) ::
      Nil)

  val outputType = StructType(
    StructField("login_cnt", IntegerType) ::StructField("logout_cnt", IntegerType) ::
      StructField("login_seq", IntegerType) ::StructField("logout_seq", IntegerType) ::
      StructField("h_count", IntegerType) ::
      StructField("re_send_cnt", IntegerType) ::
      StructField("send_cnt", IntegerType) ::
      StructField("lvl_cnt", IntegerType) ::
      StructField("st", LongType) ::
      StructField("et", LongType) ::
      Nil)

  override def inputSchema: StructType = InPutType

  override def bufferSchema: StructType = outputType

  override def dataType: DataType = {
    StructType(
      StructField("v_flag", IntegerType) ::
        StructField("g_flag", IntegerType) ::
        StructField("h_count", IntegerType) ::
        StructField("re_send_cnt", IntegerType) ::
        StructField("send_cnt", IntegerType) ::
        StructField("lvl_cnt", IntegerType) ::
        StructField("st", LongType) ::
        StructField("et", LongType) ::
        StructField("login_seq", IntegerType) ::
        StructField("logout_seq", IntegerType) ::
        Nil)
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
    buffer(2) = -1
    buffer(3) = -1
    buffer(4) = 0
    buffer(5) = 0
    buffer(6) = 0
    buffer(7) = 0

    buffer(8) = -1l
    buffer(9) = -1l
  }

  override def update(buffer: MutableAggregationBuffer, e: Row): Unit = {
    val ts = e.getLong(0)
    val command_flag = if(e.isNullAt(1)) -1 else e.getInt(1)
    val login_seq = if(e.isNullAt(2)) -1 else e.getInt(2)
    val logout_seq = if(e.isNullAt(3)) -1 else e.getInt(3)
    val lvl = if(e.isNullAt(4)) 0 else e.getInt(4)

    if(command_flag == 1){
      buffer(0) = buffer.getInt(0) + 1
      buffer(2) = login_seq
      buffer(8) = ts
    }
    if(command_flag == 4){
      buffer(1) = buffer.getInt(1) + 1
      buffer(3) = logout_seq
      buffer(9) = ts
    }

    if(command_flag == 2){
      buffer(6) = buffer.getInt(6) + 1
    }
    if(command_flag == 3){
      buffer(5) = buffer.getInt(5) + 1
    }

    if(command_flag == 7){
      buffer(4) = buffer.getInt(4) + 1
    }
    if(lvl == 3){
      buffer(7) = buffer.getInt(7) + 1
    }
  }

  override def merge(b1: MutableAggregationBuffer, b2: Row): Unit = {
    b1(0) = b1.getInt(0) + b2.getInt(0)
    b1(1) = b1.getInt(1) + b2.getInt(1)

    b1(4) = b1.getInt(4) + b2.getInt(4)
    b1(5) = b1.getInt(5) + b2.getInt(5)
    b1(6) = b1.getInt(6) + b2.getInt(6)
    b1(7) = b1.getInt(7) + b2.getInt(7)

    if(b2.getInt(2) != -1){
      b1(2) = b2.getInt(2)
    }

    if(b2.getInt(3) != -1){
      b1(3) = b2.getInt(3)
    }

    if(b2.getLong(8) != -1l){
      b1(8) = b2.getLong(8)
    }

    if(b2.getLong(9) != -1l){
      b1(9) = b2.getLong(9)
    }
  }


  override def evaluate(b: Row): Any = {
    val login = b.getInt(0)
    val logout = b.getInt(1)

    val login_seq = b.getInt(2)
    val logout_seq = b.getInt(3)

    val st = b.getLong(8)
    val et = b.getLong(9)

    val send = b.getInt(6)
    val re_send = b.getInt(5)
    val h = b.getInt(4)
    val lvl = b.getInt(7)

    val res = if(login>0 && logout>0){
      if(login_seq == logout_seq){
        Row(0,0,h,re_send,send,lvl,st,et, login_seq,logout_seq)
      }else{
        Row(1,0,h,re_send,send,lvl,st,et , login_seq,logout_seq)
      }
    }else if(login>0 && logout == 0){
      Row(1,1,h,re_send,send,lvl,st,-1l, login_seq,logout_seq )
    }else if(login == 0 && logout > 0){
      Row(1,2,h,re_send,send,lvl,-1l,et , login_seq,logout_seq)
    }else{
      Row(1,3,h,re_send,send,lvl,-1l,-1l , login_seq,logout_seq)
    }
    res
  }
}
