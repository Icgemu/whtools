package cn.gaei.wh

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class LossStats extends UserDefinedAggregateFunction {

  case class SpdInput(ts:Long, command_flag:Int, login_seq:Int, logout_seq:Int,
                      lvl:Int)

  val InPutType = StructType(
    StructField("ts", LongType) ::
      StructField("command_flag", IntegerType) ::
      StructField("login_seq", IntegerType) ::
      StructField("logout_seq", IntegerType) ::
      StructField("lvl", IntegerType) ::
      Nil)

  override def inputSchema: StructType = InPutType

  override def bufferSchema: StructType = {
    StructType(
      StructField("data", DataTypes.createArrayType(InPutType)) ::
        Nil)
  }


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
      val command_flag = if(e.isNullAt(1)) -1 else e.getInt(1)
      val login_seq = if(e.isNullAt(2)) -1 else e.getInt(2)
      val logout_seq = if(e.isNullAt(3)) -1 else e.getInt(3)
      val lvl = if(e.isNullAt(4)) 0 else e.getInt(4)

      SpdInput(ts, command_flag, login_seq,logout_seq, lvl)
    })

    val login = data.filter(_.command_flag == 1)
    val logout = data.filter(_.command_flag == 4)

    val send = data.filter(_.command_flag == 2).size
    val re_send = data.filter(_.command_flag == 3).size
    val h = data.filter(_.command_flag == 7).size
    val lvl = data.filter(_.lvl == 3).size

    val res = if(login.size>0 && logout.size>0){
      if(login.head.login_seq == logout.last.logout_seq){
        Row(0,0,h,re_send,send,lvl,login.head.ts,logout.last.ts )
      }else{
        Row(1,0,h,re_send,send,lvl,login.head.ts,logout.last.ts )
      }
    }else if(login.size>0 && logout.size == 0){
      Row(1,1,h,re_send,send,lvl,login.head.ts,-1l )
    }else if(login.size>0 && logout.size == 0){
      Row(1,2,h,re_send,send,lvl,-1l,logout.last.ts )
    }else{
      Row(1,3,h,re_send,send,lvl,-1l,-1l )
    }
    res
  }
}
