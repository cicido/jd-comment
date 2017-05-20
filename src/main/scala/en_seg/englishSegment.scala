package en_seg

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/5/16.
  */

object englishSegment {
  val srcTable = "uxip.ads_rpt_uxip_hotapps_relate_suggest_d"
  val dstTable = "algo.tlc_jd_hotapps_relate_suggest"
  val sparkEnv = new SparkEnv("SingleWordMatch")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    //val wordpattern = Pattern.compile("[ \t\n\r]*()")

    val selectSQL = s"select pak_id,text_detail from ${srcTable} where stat_date=${dt}"
    val selectRDD = sparkEnv.hiveContext.sql(selectSQL).repartition(200).map(r=>{
      val pak_id = r.getAs[String](0)
      val text_detail = r.getAs[String](1)
      val is_all_english = {
        if (text_detail.filter(w => {
            !(w > 0 && w < 128) &&
            !(w>8209 && w<12306) &&
            !(w > 65077 && w<65508)
        }).length > 0)
          0
        else
          1
      }
      (pak_id,is_all_english,text_detail)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      selectRDD.toDF("pak_id","tex_detail","english_text")
    }
    DXPUtils.saveDataFrame(trDF,dstTable,dt,sparkEnv.hiveContext)
  }
}
