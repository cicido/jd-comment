package classmatch

import breeze.linalg.split
import common.SegWordUtils

/**
  * Created by duanxiping on 2017/5/16.
  */
object MannualWordMatch {
  val mannualFile = "/tmp/duanxiping/jd-keywords/keywords.txt"
  val srcTable = "project.bdl_spider_jd_product_comment_new"
  val dstTable = "algo.dxp_jd_product_comment_class"
  val sparkEnv = new SparkEnv("MannualWordMatch")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    // 获取售后收集的关键词表,形成主体词+描述词对
    val word2Class = sparkEnv.sc.textFile(mannualFile).
      filter(_.split("\t").length == 5).flatMap(r=>{
      val fieldArr = r.split("\t")
      val fullclass = fieldArr.slice(0,2).mkString("-")

      val mwords = fieldArr(2).split("，").map(w=>w.trim)
      val dwords =
        if(fieldArr(3)=="" || fieldArr(3).length == 0)
          Array[String]()
        else
          fieldArr(3).split("，").map(w=>w.trim)

      mwords.flatMap(w=>{
          if(dwords.length == 0)
            Array(w,fullclass)
          else
            dwords.map(x=>(w+":"+x,fullclass))
      })
    })


    val mword2Class = word2Class.flatMap(r=>{
      r._1._1.map(w=>(w,Set(r._2)))
    }).reduceByKey(_ ++ _).collectAsMap()

    val dword2Class = word2Class.flatMap(r=>{
      r._1._2.map(w=>(w,Set(r._2)))
    }).reduceByKey(_ ++ _).collectAsMap()

    val mword2ClassBr = sparkEnv.sc.broadcast(mword2Class)
    val dword2ClassBr = sparkEnv.sc.broadcast(dword2Class)


    //val word2ClassBr = sparkEnv.sc.broadcast(word2Class)
    val commentSQL = s"select commentid, comment, nature from " +
      s"${srcTable} where dt=20170511 and nature < 1 limit 100"
    val commentRDD = sparkEnv.hiveContext.sql(commentSQL).repartition(200).map(r=>{
      val mwordClassMap = mword2ClassBr.value
      val dwordClassMap = dword2ClassBr.value

      val commentid = r.getAs[String](0)
      val nature = r.getAs[Int](2)
      val comment = r.getAs[String](1)
      val words = SegWordUtils.segMsgWithNature(comment).filter(w=>{
        w._2 == "mzm" || w._2 == "mzd"
      })
      val mwords = words.filter(_._2 == "mzm").flatMap(w=>{
          if(mwordClassMap.contains(w._1))
            mwordClassMap(w._1).toArray
          else
            Array("")
        }).filter(_ !="")
      })


      val mid = words.map(w=>{

      })

    })
  }
}
