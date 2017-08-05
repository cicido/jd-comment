package classmatch

import classifier.DMNBTextClassifierOpt
import common.{DXPUtils, SegWordUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import weka.core.Instance

/**
  * Created by duanxiping on 2017/5/16.
  */
object SingleWordMatch {
  // 关键词文件
  val mannualFile = "/tmp/duanxiping/jd/keywords.txt"
  // 模型训练文件
  val trainFile = "/tmp/duanxiping/jd/jd-train-data.txt"
  //val srcTable = "project.bdl_spider_jd_product_comment_new"
  val srcTable = "project.bdl_spider_jd_product_comment_new2"
  //val dstTable = "algo.dxp_jd_product_comment_keyclass_predclass"
  val dstTable = "algo.dxp_jd_product_class"
  val sparkEnv = new SparkEnv("SingleWordMatch")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    // 获取售后收集的关键词表,形成(主体词+描述词对,class)
    // 至少有三个列
    val noneType = "无理由-无理由或描述不清"

    val word2Class = sparkEnv.sc.textFile(mannualFile).
      filter(_.split("\t").length > 2).map(r => {
      val fieldArr = r.split("\t", 3)
      //最多三个子串,主类,二级分类,主体词或描述词
      val fullclass = fieldArr.slice(0, 2).mkString("-")
      //业务需要二级分类slice(0,2),只需大类用slice(0,1)
      val mdwords = fieldArr(2).trim.split("[\t ]+", 2)
      //主体词与描述词可能不以\t分割
      val mwords = mdwords(0).split("，").map(w => w.trim)
      val dwords =
        if (mdwords.length < 2)
          Array[String]()
        else
          mdwords(1).split("，").map(w => w.trim)
      ((mwords.filter(_.length > 0), dwords.filter(_.length > 0)), fullclass)
    })

    //test
    println("*" * 40)
    println("print word2Class")
    word2Class.collect().foreach(r => {
      println(r._1._1.mkString(","))
      println(r._1._2.mkString(","))
      println(r._2)
      println("*" * 40)
    })

    val mword2Class = word2Class.flatMap(r => {
      r._1._1.map(w => (w, Array(r._2)))
    }).reduceByKey(_ ++ _).collectAsMap()

    val dword2Class = word2Class.flatMap(r => {
      r._1._2.map(w => (w, Array(r._2)))
    }).reduceByKey(_ ++ _).collectAsMap()

    println("*" * 40)
    println("print mword2class")
    mword2Class.foreach(r => {
      println(r._1 + "->" + r._2.mkString(","))
    })

    println("*" * 40)
    println("print dword2class")
    dword2Class.foreach(r => {
      println(r._1 + "->" + r._2.mkString(","))
    })

    val stopWords: Set[String] = mword2Class.filter(_._2.toSet.size > 1)
      .map(_._1).toSet | dword2Class.filter(_._2.toSet.size > 1).map(_._1).toSet
    println("*"*40)
    println("print stop words:")
    stopWords.foreach(r=>{
      println(r)
    })

    val mword2ClassBr = sparkEnv.sc.broadcast(mword2Class)
    val dword2ClassBr = sparkEnv.sc.broadcast(dword2Class)

    val word2ClassBr = sparkEnv.sc.broadcast(word2Class)

    // 加载模型训练文件
    val trainData:Array[(Array[String],String)] = sparkEnv.sc.textFile(trainFile).
      filter(_.length > 0).map(r => {
      val data = r;
      val line = if (data(0) == '\uFEFF' || data(0) == '\uFFFE') data.substring(1) else data
      val idx = line.indexOf('\t')
      if (idx != -1 && idx + 1 < line.length) {
        val id = line.substring(0, idx)
        val msg = line.substring(idx + 1)
        (msg, id)
      } else {
        (r, "")
      }
    }).filter(_._2.length > 0).map(r => {
      val words = SegWordUtils.segMsg(r._1).filterNot(stopWords.contains(_))
      (words, r._2)
    }).filter(_._1.length > 0).collect()

    val classifier:DMNBTextClassifierOpt = new DMNBTextClassifierOpt(50)
    classifier.trainClassifier(trainData)

    val commentSQL = s"select commentid, content,nature,devicecode,brandcode,storename,storeid from " +
      s"${srcTable} where dt=${dt} and nature < 1"

    val commentRDD = sparkEnv.hiveContext.sql(commentSQL).repartition(200).map(r => {
      val mwordClassMap = mword2ClassBr.value
      val dwordClassMap = dword2ClassBr.value

      //val word2Class = word2ClassBr.value
      val commentid = r.getAs[String](0)
      val nature = r.getAs[Int](2)
      val content = r.getAs[String](1)

      val words = SegWordUtils.segMsgWithNature(content).map(_._1)
      /*.filter(w=>{
              w._2 == "mzm" || w._2 == "mzd"
            })*/

      val mwords = words.filter(mwordClassMap.contains(_))
      val dwords = words.filter(dwordClassMap.contains(_))

      val keyClassArr =
        if (mwords.length == 0 && dwords.length == 0)
          Array(noneType)
        else if (mwords.length > 0 && dwords.length == 0) {
          val oneClass = mwords.filter(!stopWords.contains(_))
          if (oneClass.length == 0)
            Array(noneType)
          else
            oneClass.flatMap(mwordClassMap(_)).toSet.toArray
        }
        else if (dwords.length > 0 && mwords.length == 0) {
          val oneClass = dwords.filter(!stopWords.contains(_))
          if (oneClass.length == 0)
            Array(noneType)
          else
            oneClass.flatMap(dwordClassMap(_)).toSet.toArray
        }
        else {
          val mClass = mwords.flatMap(mwordClassMap(_))
          val dClass = dwords.flatMap(dwordClassMap(_))
          val mdSet = mClass.toSet & dClass.toSet
          if (mdSet.size > 0)
            mdSet.toArray
          else {
            val mfiltered = mwords.filter(!stopWords.contains(_))
            val dfiltered = dwords.filter(!stopWords.contains(_))
              if((mfiltered ++ dfiltered).length == 0){
                Array(noneType)
              }else {
                (mfiltered.flatMap(mwordClassMap(_)) ++ dfiltered.flatMap(dwordClassMap(_)))
                  .map(r => (r, 1)).groupBy(_._1).
                  map(l => (l._1, l._2.map(_._2).reduce(_ + _)))
                  .toArray.sortWith(_._2 > _._2).take(3).map(_._1)
              }
          }
        }

      // 模型预测
      val inst: Instance = classifier.createSparseInstance(words, false)
      inst.setDataset(classifier.m_structure)
      val res: Array[Double] = classifier.distributionForInstance(inst)
      val cvalue: Array[Double] = res.sortWith(_ > _).take(3)
      val classid: Array[String] = cvalue.map(r => classifier.m_structure.
        classAttribute().value(res.indexOf(r)))


      // 合并关键词类别与模型预测类别
      val mergeSet = keyClassArr.toSet & classid.toSet
      val mergeClass = if (mergeSet.size > 0) {
        if(mergeSet.contains(classid(0)))
          mergeSet.toArray
        else if(cvalue(0) > 0.7)
          classid.take(1)
        else
          mergeSet.toArray
      } else if (cvalue(0) > 0.9) {
        classid.take(1)
      } else if (keyClassArr(0) == noneType && cvalue(0) > 0.10) {
        classid.take(1)
      } else{
        keyClassArr
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

      /*(commentid, nature, content, mwords.mkString(",") + "|" + dwords.mkString(","),
        mergeClass.mkString(","),
        keyClassArr.mkString(","),
        classid.mkString(","),
        cvalue(0).toString,
        devicecode, brandcode, storename, storeid)
        */
      (commentid, nature, content, mwords.mkString(",") + "|" + dwords.mkString(","),
        mergeClass.mkString(","),
        devicecode, brandcode, storename, storeid)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      /*commentRDD.toDF("commentid", "nature", "content", "words","mergeclass",
        "keyclass","predclass","cvalue",
        "devicecode", "brandcode", "storename", "storeid")
        */
      commentRDD.toDF("commentid", "nature", "content", "words","class",
        "devicecode", "brandcode", "storename", "storeid").repartition(1)
    }
    DXPUtils.saveDataFrame(trDF, dstTable, dt, sparkEnv.hiveContext)
  }
}
