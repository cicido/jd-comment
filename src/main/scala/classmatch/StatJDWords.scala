package classmatch

import common.{DXPUtils, SegWordUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/5/16.
  */
object StatJDWords {
  val mannualFile = "/tmp/duanxiping/jd-keywords/keywords.txt"
  val srcTable = "project.bdl_spider_jd_product_comment_new2"
  val dstTable = "algo.dxp_jd_product_word_nature_cnt"
  val sparkEnv = new SparkEnv("StatJDWords")

  def main(args: Array[String]): Unit = {
    val dt = args(0)

    val commentSQL = s"select commentid, content, nature from ${srcTable} where nature < 1"
    val commentRDD = sparkEnv.hiveContext.sql(commentSQL).repartition(400).flatMap(r=>{
      /*
      val commentid = r.getAs[String](0)
      val nature = r.getAs[Int](2)
      */
      val comment = r.getAs[String](1)
      val words = SegWordUtils.segMsgWithNature(comment).filterNot(w=>{
        w._2.startsWith("mzd") || w._2.startsWith("mzm") || w._2.startsWith("w")
      })
      val mword = words.filter(w=>{
        w._2.startsWith("n")
      })
      words.filter(w=>{
        w._2.startsWith("a") || (w._2.startsWith("v") && w._2 != "vshi")
      }).flatMap(w=>{
        mword.map(m=>((m._1+"-"+ w._1,m._2+"-"+w._2),1))
      })
    }).reduceByKey(_+_).map(r=>{
      (r._1._1,r._1._2,r._2)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      commentRDD.toDF("words","nature","cnt")
    }
    DXPUtils.saveDataFrame(trDF,dstTable,dt,sparkEnv.hiveContext)
  }
}
