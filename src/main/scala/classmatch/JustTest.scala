package classmatch

import common.{DXPUtils, SegWordUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/5/16.
  */
object JustTest {
  val mannualFile = "/tmp/duanxiping/jd-keywords/keywords.txt"
  val dstTable = "algo.dxp_jd_product_comment_class_test2"
  val sparkEnv = new SparkEnv("MannualWordMatch")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    // 获取售后收集的关键词表,形成(主体词+描述词对,class)
    val word2Class = sparkEnv.sc.textFile(mannualFile).
      filter(_.split("\t").length > 5).map(r=>{
      val fieldArr = r.split("\t")
      (fieldArr(0),fieldArr(1))
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      word2Class.toDF("a","b")
    }
    DXPUtils.saveDataFrame(trDF,dstTable,dt,sparkEnv.hiveContext)

  }
}
