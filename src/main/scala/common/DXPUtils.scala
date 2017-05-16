package common

import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

/**
  * Created by duanxiping on 2017/2/5.
  */
object DXPUtils {

  // 在实际建表时,可能需要灵活的分区类型,如string类型
  // 增加partition分区类型设置,不改动原有的默认bigint类型
  def saveDataFrame(df: DataFrame, outTable: String, dt: String,
                    hiveContext: HiveContext): Unit ={
    saveDataFrameWithType(df,outTable,dt,hiveContext,"bigint")
  }
  def saveDataFrameWithType(df: DataFrame, outTable: String, dt: String,
                    hiveContext: HiveContext, partionType:String): Unit = {
    val cols = df.columns
    val sma = df.schema
    val colsType = cols.map(r => {
      sma(r).dataType match {
        case IntegerType => "int"
        case LongType => "bigint"
        case StringType => "string"
        case BooleanType => "boolean"
        case DoubleType => "double"
      }
    })

    val colsString = cols.zip(colsType).map(r => r._1 + " " + r._2).mkString(",")
    val create_table_sql: String = s"create table if not exists $outTable " +
      s" ($colsString) partitioned by (stat_date ${partionType}) STORED AS RCFILE"
    println(create_table_sql)
    hiveContext.sql(create_table_sql)

    val tmptable = "dxp_tmp_table"
    df.registerTempTable(tmptable)

    val outdt = if(partionType == "string") "\"" + dt +"\"" else dt

    val insert_sql: String = s"insert overwrite table $outTable partition(stat_date = $outdt) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
    hiveContext.dropTempTable(tmptable)
  }

  // 增加全半角转换及繁简体转换
  def segMsg(msg: String): Array[String] = {
    val simpleMsg= HanLP.convertToSimplifiedChinese(toSBC(msg))
    HanLP.segment(simpleMsg).map(r => {
      r.word
    }).toArray
  }

  def segMsgWithNature(msg: String): Array[(String, String)] = {
    val simpleMsg= HanLP.convertToSimplifiedChinese(toSBC(msg))
    HanLP.segment(simpleMsg).map(r => {
      (r.word, r.nature.toString)
    }).toArray
  }

  def toSBC(input: String): String = {
    val c: Array[Char] = input.toCharArray
    val d = c.map(r => {
      if (r == '\u3000') ' '
      else if (r < '\uFF5F' && r > '\uFF00') {
        (r - 65248).toChar
      }
      else r
    })
    new String(d)
  }

  def main(args: Array[String]): Unit = {
    println(toSBC("ＮＩＡＥＰＮＩＡＥＰ"))
  }
}
