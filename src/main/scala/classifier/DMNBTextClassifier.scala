package classifier

import java.io.{File, PrintWriter}
import java.nio.charset.MalformedInputException
import java.util.{HashMap => HMap, List => JList}

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.{Term => HanLPTerm}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import weka.classifiers.bayes.DMNBtext
import weka.core._

import scala.collection.JavaConversions._
import scala.io.Source


object DMNBTextClassifier {
  //spark,hive
  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null

  val src_table = "project.edl_owl_reply_merge_di"
  val dst_table = "project.dxp_edl_owl_reply_classfier"




  def getDataFromHive(table:String, stat_date:BigInt, stat_hour:BigInt):DataFrame = {
    val select_sql = s"select id, content from $table where stat_date=$stat_date and stat_hour=$stat_hour and content is not null and length(content)>2"
    hiveContext.sql(select_sql)
  }

  def predictDataFromHive(classifier:DMNBTextClassifier,stat_date:BigInt, stat_hour:BigInt): Unit ={
    val test_df:DataFrame = getDataFromHive(src_table, stat_date, stat_hour)
    val classified_data_rdd = test_df.repartition(500).map(r=>{
      val id =  r.getAs[String]("id")
      val msg = r.getAs[String]("content")
      println(id+","+msg)
      var classid:String = "-100"
      var cvalue: Double = 0.0
      val (wordlist,inst) = classifier.createSparseInstance(msg, false)
      if (wordlist.length != 0){
        inst.setDataset(classifier.m_structure)
        val res: Array[Double] = classifier.distributionForInstance(inst)
        cvalue = res.max
        classid = classifier.m_structure.classAttribute().value(res.indexOf(cvalue))
      }
      println(Array(id,classid,cvalue,msg).mkString("\t"))
      Row(id,classid,cvalue,msg)
    })

    //print data
    val st = StructType(
      StructField("id", StringType, false) ::
        StructField("classid", StringType, false) ::
        StructField("cvalue", DoubleType, false) ::
        StructField("content", StringType, false) :: Nil)

    val tmptable: String = "dxp_tmp_table"
    hiveContext.createDataFrame(classified_data_rdd, st).registerTempTable(tmptable)

    val create_table_sql: String = s"create table if not exists $dst_table " +
      "(id string, classid string, cvalue double, content string) partitioned by " +
      "(stat_date bigint,stat_hour bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $dst_table " +
      s"partition(stat_date = $stat_date,stat_hour=$stat_hour) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
  }

  def initSpark(appname: String): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    hiveContext = new HiveContext(sc)
    sqlContext = new SQLContext(sc)
  }

  //try to open file with different decode format
  def tryOpenFile(filename: String): Array[String] = {
    try {
      Source.fromFile(filename, "utf8").getLines().toArray
    } catch {
      case e: MalformedInputException => Source.fromFile(filename, "cp936").getLines().toArray
    }
  }

  def predictDataFromFile(classifier:DMNBTextClassifier, fname:String, hasLabel:Boolean): Unit ={
    val testOri:Array[String] = tryOpenFile(fname)
    val test:Array[(String,String)] = if(hasLabel){
      testOri.filter(_.length > 0).map(r=>{
        val data = r;
        val line = if (data(0) == '\uFEFF' || data(0) == '\uFFFE') data.substring(1) else data
        val idx = line.indexOf('\t')
        if (idx != -1 && idx + 1 < line.length) {
          val id = line.substring(0, idx)
          val msg = line.substring(idx + 1)
          (msg,id)
        }else{
          (r,"noclassid")
        }
      })
    }else{
      testOri.map(r=>{
        (r,"-100")
      })
    }
    val writer = new PrintWriter(new File("test.txt"))
    test.foreach(r=>{
      var classid:String = "-100"
      var cvalue: Double = 0.0
      val (wordlist,inst) = classifier.createSparseInstance(r._1, false)
      if (wordlist.length != 0){
        inst.setDataset(classifier.m_structure)
        val res: Array[Double] = classifier.distributionForInstance(inst)
        cvalue = res.max
        classid = classifier.m_structure.classAttribute().value(res.indexOf(cvalue))
      }
      println(Array(r._2,classid,cvalue,r).mkString(","))
      writer.write(Array(r._2,classid,cvalue,r._1,"\n").mkString("\t"))
    })
    writer.close()
  }
}

class DMNBTextClassifier(tranFile:String) extends Serializable{
  //data structure
  var m_structure: Instances = null
  val m_indexDict: HMap[String, Attribute] = new HMap[String,Attribute]()
  var m_indexWord: HMap[String, Int] = new HMap[String,Int]()
  var m_dmnbClassifier = new DMNBtext
  trainClassifier(tranFile)

  def distributionForInstance(inst:Instance): Array[Double] ={
    m_dmnbClassifier.distributionForInstance(inst)
  }

  def trainClassifier(traiFile:String) {
    val train: HMap[String, String] = getData(traiFile)
    createStructure(train)
    val instances: Instances = new Instances(m_structure)
    //println(instances)
    var i = 0
    train.foreach(r => {
      instances.add(createSparseInstance(r._1, r._2, false)._2)
      i += 1
      if (i % 1000 == 0) println("creatInstance:" + i)
    })
    println("start building...")
    m_dmnbClassifier.buildClassifier(instances)
    println("complete build!!")
  }

  // instances attribute
  def createStructure(oridata: HMap[String, String]) {
    val segdata: HMap[Array[String], (String, String)] = segData(oridata)
    val wordmap: HMap[String, Double] = selectWord(segdata)
    val attributes: FastVector = new FastVector(wordmap.size)
    wordmap.foreach(r => {
      attributes.addElement(new Attribute(r._1))
    })

    //class value
    val classIDs = segdata.map(r=>{
      r._2._2
    }).toSet

    val my_nominal_values: FastVector = new FastVector(classIDs.size + 1)
    my_nominal_values.addElement("dummy")
    classIDs.foreach(r => {
      my_nominal_values.addElement(r)
    })

    val classAttr: Attribute = new Attribute("class attribute", my_nominal_values)
    attributes.addElement(classAttr)

    m_structure = new Instances("filter bad message", attributes, 0)
    m_structure.setClass(classAttr)

    val numAttr = m_structure.numAttributes()
    for (i <- 0 until numAttr) {
      m_indexDict(m_structure.attribute(i).name()) = m_structure.attribute(i)
      m_indexWord(m_structure.attribute(i).name()) = m_structure.attribute(i).index()
    }
  }


  //map get rid of  duplicate
  def getData(filename: String): HMap[String,String] = {
    val train = new HMap[String, String]()
    val alldata = DMNBTextClassifier.tryOpenFile(filename)
    for (data <- alldata if data.length > 0) {
      val line = if (data(0) == '\uFEFF' || data(0) == '\uFFFE') data.substring(1) else data
      val idx = line.indexOf('\t')
      if (idx != -1 && idx + 1 < line.length) {
        val id = line.substring(0, idx)
        val msg = line.substring(idx + 1)
        train(msg) = id
      }
    }
    train
  }

  // return: HMap[wordlist,(msg, classid)]
  def segData(data: HMap[String, String]): HMap[Array[String], (String, String)] = {
    val segdata = new HMap[Array[String], (String, String)]()
    data.foreach(r => {
      val termlist: JList[HanLPTerm] = HanLP.segment(r._1)
      val wordlist = scala.collection.mutable.ArrayBuffer.empty[String]
      for (term: HanLPTerm <- termlist) {
        wordlist += term.word
      }
      segdata(wordlist.toArray) = (r._1, r._2)
    })
    segdata
  }

  def segMsg(msg: String): Array[String] = {
    val wordlist = scala.collection.mutable.ArrayBuffer.empty[String]
    for (term: HanLPTerm <- HanLP.segment(msg)) {
      wordlist += term.word
    }
    wordlist.toArray
  }

  def selectWord(data: HMap[Array[String], (String, String)]): HMap[String, Double] = {
    val wordmap = new HMap[String, HMap[String, Int]]()
    val featureMap = new HMap[String, Double]
    data.foreach(r => {
      val cid = r._2._2
      r._1.foreach(w => {
        //letter string is less than 10
        if (w.length < 20) {
          if (wordmap.contains(w)) {
            wordmap(w)(cid) = if (wordmap(w).contains(cid)) wordmap(w)(cid) + 1 else 1
          } else {
            wordmap(w) = new HMap[String, Int]()
            wordmap(w)(cid) = 1
          }
        }
      })
    })

    //select word
    val stopWords = Array("的", "很", "啊","手机", "吧", "呀", "了","@qq,com","@","qq","com","QQ")
    wordmap.foreach(r => {
      val cidmap: HMap[String, Int] = r._2
      val class_cnt: Int = cidmap.keys.size
      val all_cnt: Int = cidmap.values().sum
      /*
      val avg:Double = all_cnt.toDouble/class_cnt.toDouble
      var std:Double = 0
      cidmap.foreach(r=>{
        std += (r._2-avg)*(r._2-avg)
      })
      std = std/(all_cnt*all_cnt)
      */
      if (!stopWords.contains(r._1) && (all_cnt > 0 || cidmap.contains("1"))) {
        featureMap(r._1) = all_cnt
      }
    })
    featureMap
  }

  def createSparseInstance(msg:String, needCount:Boolean): (Array[String],Instance) ={
    val numAttr = m_structure.numAttributes()
    val attrValues: Array[Double] = new Array[Double](numAttr)
    val wordMap: HMap[String, Int] = new HMap[String, Int]()
    segMsg(msg).filter(r => {
      m_indexWord.contains(r)
    }).foreach(r => {
      wordMap(r) = if (wordMap.contains(r)) 1 + wordMap(r) else 1
    })

    wordMap.foreach(w => {
      if (needCount) {
        attrValues(m_indexWord(w._1)) = w._2
      } else {
        attrValues(m_indexWord(w._1)) = 1
      }
    })
    val wordlist: Array[String] = wordMap.map(r => {
      r._1
    }).toArray
    (wordlist,new SparseInstance(1, attrValues))
  }

  def createSparseInstance(msg: String, classid: String, needCount: Boolean): (Array[String], Instance) = {
    val (wordlist, inst) = createSparseInstance(msg,needCount)
    inst.setValue(m_structure.classAttribute(), classid)
    (wordlist, inst)
  }

  def createInstance(msg: String, classid: Int, needCount: Boolean): (Array[String], Instance) = {
    val numAttr = m_structure.numAttributes()
    val inst: Instance = new Instance(numAttr)
    for (i <- 0 until numAttr) {
      inst.setValue(m_structure.attribute(i), 0)
    }
    //set class value
    inst.setValue(m_structure.classAttribute(), classid.toString)
    val wordlist: Array[String] = segMsg(msg)
    wordlist.foreach(w => {
      if (m_indexDict.contains(w)) {
        if (needCount) {
          inst.setValue(m_indexDict(w), 1 + inst.value(m_indexDict(w)))
        } else {
          inst.setValue(m_indexDict(w), 1)
        }
      }
    })
    (wordlist, new SparseInstance(inst))
  }

}
