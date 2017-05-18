package classmatch

import common.{DXPUtils, SegWordUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/5/16.
  */
object SingleWordMatch {
  val mannualFile = "/tmp/duanxiping/jd-keywords/keywords.txt"
  val srcTable = "project.bdl_spider_jd_product_comment_new"
  val dstTable = "algo.dxp_jd_product_comment_class"
  val sparkEnv = new SparkEnv("SingleWordMatch")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    // 获取售后收集的关键词表,形成(主体词+描述词对,class)
    println("*"*40)
    println("compute fields length sepertor by tab")
    sparkEnv.sc.textFile(mannualFile).
      map(r=>{
        val flen = r.split("\t").length
        (r,flen)
      }).collect().foreach(r=>{
      println(r._2.toString + "\t"+ r._1)
    })

    val word2Class = sparkEnv.sc.textFile(mannualFile).
      filter(_.split("\t").length > 2).map(r=>{
      val fieldArr = r.split("\t")
      val fullclass = fieldArr.slice(0,2).mkString("-")

      val mwords = fieldArr(2).split("，").map(w=>w.trim)

      val dwords =
        if(fieldArr.length < 4 || fieldArr(3)=="" || fieldArr(3).length == 0)
          Array[String]()
        else
          fieldArr(3).split("，").map(w=>w.trim)
      ((mwords,dwords),fullclass)
      })

    println("*"*40)
    println("print word2Class")
    word2Class.collect().foreach(r=>{
      println(r._1._1.mkString(","))
      println(r._1._2.mkString(","))
      println(r._2)
      println("*"*40)
    })

    val mword2Class = word2Class.flatMap(r=>{
      r._1._1.map(w=>(w,Array(r._2)))
    }).reduceByKey(_ ++ _).collectAsMap()

    val dword2Class = word2Class.flatMap(r=>{
      r._1._2.map(w=>(w,Array(r._2)))
    }).reduceByKey(_ ++ _).collectAsMap()

    println("*"*40)
    println("print mword2class")
    mword2Class.foreach(r=>{
      println(r._1 + "->" + r._2.mkString(","))
    })

    println("*"*40)
    println("print dword2class")
    dword2Class.foreach(r=>{
      println(r._1 + "->" + r._2.mkString(","))
    })

    val mword2ClassBr = sparkEnv.sc.broadcast(mword2Class)
    val dword2ClassBr = sparkEnv.sc.broadcast(dword2Class)

    val word2ClassBr = sparkEnv.sc.broadcast(word2Class)
    val commentSQL = s"select commentid, content, nature from " +
      s"${srcTable} where dt=${dt} and nature < 1 limit 10000"
    val commentRDD = sparkEnv.hiveContext.sql(commentSQL).repartition(200).map(r=>{

      val mwordClassMap = mword2ClassBr.value
      val dwordClassMap = dword2ClassBr.value

      //val word2Class = word2ClassBr.value
      val commentid = r.getAs[String](0)
      val nature = r.getAs[Int](2)
      val content = r.getAs[String](1)
      val words = SegWordUtils.segMsgWithNature(content).filter(w=>{
        w._2 == "mzm" || w._2 == "mzd"
      })
      val mwords = words.filter(_._2 == "mzm").flatMap(w=>{
        mwordClassMap(w._1)
      })
      val dwords = words.filter(_._2 == "mzd").flatMap(w=>{
        dwordClassMap(w._1)
      })

      val classStr =
        if(mwords.length == 0 && dwords.length == 0)
          "none"
        else if(mwords.length > 0 && dwords.length == 0)
          mwords.toSet.mkString(",")
        else if(mwords.length == 0 && dwords.length > 0)
          dwords.toSet.mkString(",")
        else{
          val mdSet = mwords.toSet & dwords.toSet
          if(mdSet.size == 0)
            mwords.toSet.mkString(",")
          else
            mdSet.mkString(",")
        }

      (commentid,nature,content,words.map(r=>r._1+"-"+r._2).mkString(","),classStr)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      commentRDD.toDF("commentid","nature","content","words","class")
    }
    DXPUtils.saveDataFrame(trDF,dstTable,dt,sparkEnv.hiveContext)

  }
}
