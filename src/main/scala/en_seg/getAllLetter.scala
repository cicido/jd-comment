package en_seg

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/5/16.
  */

object getAllLetter {
  val srcTable = "uxip.ads_rpt_uxip_hotapps_relate_suggest_d"
  val dstTable = "algo.tlc_jd_hotapps_relate_suggest_letters"
  val sparkEnv = new SparkEnv("SingleWordMatch")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    //val wordpattern = Pattern.compile("[ \t\n\r]*()")

    val selectSQL = s"select pak_id,text_detail from ${srcTable} where stat_date=${dt}"
    val selectRDD = sparkEnv.hiveContext.sql(selectSQL).repartition(200).flatMap(r=>{
      val pak_id = r.getAs[String](0)
      r.getAs[String](1).map(s=>{
        ((s.toString,s.toInt),1)
      })
    }).reduceByKey(_ + _)

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      selectRDD.map(r=>(r._1._1,r._1._2,r._2)).filter(r=>{
        r._2 < 0 || r._2 > 31
      }).toDF("letter","pos","num")
    }
    DXPUtils.saveDataFrame(trDF,dstTable,dt,sparkEnv.hiveContext)
  }
}
