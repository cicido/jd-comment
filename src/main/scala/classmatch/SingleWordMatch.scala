package classmatch
import common.{DXPUtils, SegWordUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/5/16.
  */
object SingleWordMatch {
  val mannualFile = "/tmp/duanxiping/jd-keywords/keywords.txt"
  //val srcTable = "project.bdl_spider_jd_product_comment_new"
  val srcTable = "project.bdl_spider_jd_product_comment_new2"
  val dstTable = "algo.dxp_jd_product_comment_class"
  val sparkEnv = new SparkEnv("SingleWordMatch")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    // 获取售后收集的关键词表,形成(主体词+描述词对,class)
    /*
    println("*"*40)
    println("compute fields length sepertor by tab")
    sparkEnv.sc.textFile(mannualFile).
      map(r=>{
        val flen = r.split("\t").length
        (r,flen)
      }).collect().foreach(r=>{
      println(r._2.toString + "\t"+ r._1)
    })
    */
    val noneType = "无理由-无理由或描述不清"

    val word2Class = sparkEnv.sc.textFile(mannualFile).
      filter(_.split("\t").length > 2).map(r=>{
      val fieldArr = r.split("\t",3) //最多三个子串,主类,二级分类,主体词或描述词
      val fullclass = fieldArr.slice(0,2).mkString("-") //业务需要二级分类slice(0,2),只需大类用slice(0,1)
      val mdwords = fieldArr(2).trim.split("[\t ]+",2)  //主体词与描述词可能不以\t分割
      val mwords = mdwords(0).split("，").map(w=>w.trim)
      val dwords =
        if(mdwords.length < 2)
          Array[String]()
        else
          mdwords(1).split("，").map(w=>w.trim)
      ((mwords.filter(_.length>0),dwords.filter(_.length>0)),fullclass)
      })

    //test
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

    val stopWords:Set[String] = mword2Class.filter(_._2.toSet.size > 1)
      .map(_._1).toSet | dword2Class.filter(_._2.toSet.size > 1).map(_._1).toSet

    val mword2ClassBr = sparkEnv.sc.broadcast(mword2Class)
    val dword2ClassBr = sparkEnv.sc.broadcast(dword2Class)

    val word2ClassBr = sparkEnv.sc.broadcast(word2Class)
    val commentSQL = s"select commentid, content,nature,devicecode,brandcode,storename,storeid from " +
      s"${srcTable} where dt=${dt} and nature < 1"

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

      val mwords = words.filter(_._2 == "mzm").map(_._1)
      val dwords = words.filter(_._2 == "mzd").map(_._1)

      val mClass = mwords.flatMap(mwordClassMap(_))
      val dClass = dwords.flatMap(dwordClassMap(_))

      val classStr =
        if(mwords.length == 0 && dwords.length == 0)
          noneType
        else if(mwords.length > 0 && dwords.length == 0){
          val oneClass = mwords.filter(!stopWords.contains(_))
          if(oneClass.length == 0)
            noneType
          else
            oneClass.flatMap(mwordClassMap(_)).toSet.mkString(",")
        }
        else if(dwords.length > 0 && mwords.length == 0){
          val oneClass = dwords.filter(!stopWords.contains(_))
          if(oneClass.length == 0)
            noneType
          else
            oneClass.flatMap(dwordClassMap(_)).toSet.mkString(",")
        }
        else{
          val mdSet = mClass.toSet & dClass.toSet
          if(mdSet.size > 0)
            mdSet.mkString(",")
          else{
            (mClass ++ dClass).map(r=>(r,1)).groupBy(_._1).
              map(l => (l._1, l._2.map(_._2).reduce(_ + _)))
                .toArray.sortWith(_._2>_._2).take(2).map(_._1).mkString(",")
          }
        }
      /*
      devicecode       string  comment '机器编码',
      brandcode        string  comment '品牌标识',
      storename        string  comment '商店名称',
      storeid          string  comment '商店id',
       */
      val devicecode = r.getAs[String](3)
      val brandcode = r.getAs[String](4)
      val storename = r.getAs[String](5)
      val storeid = r.getAs[String](6)

      (commentid,nature,content,words.map(r=>r._1+"-"+r._2).mkString(","),classStr,
        devicecode,brandcode,storename,storeid)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      commentRDD.toDF("commentid","nature","content","words","class",
        "devicecode","brandcode","storename","storeid")
    }
    DXPUtils.saveDataFrame(trDF,dstTable,dt,sparkEnv.hiveContext)

  }
}
