package classifier

import java.io.{File, PrintWriter}
import java.nio.charset.MalformedInputException
import java.util.{HashMap => HMap, List => JList}

import common.{DXPUtils, SegWordUtils}
import weka.classifiers.bayes.DMNBtext
import weka.core._

import scala.collection.JavaConversions._
import scala.io.Source


/*
优化后的类里面不作分词.
输入为已分好的词序列与label.
 */
object DMNBTextClassifierOpt {
  def main(args: Array[String]): Unit = {
    val trainFile:String = args(0)
    val trainData:Array[(String,(Array[String],String))] = prepareTrainData(trainFile)
    val classifier:DMNBTextClassifierOpt = new DMNBTextClassifierOpt(50)
    classifier.trainClassifier(trainData.map(_._2))
    val cvData = trainData.map(r=>{
      val inst:Instance =  classifier.createSparseInstance(r._2._1,r._2._2,false)
      inst.setDataset(classifier.m_structure)
      val res: Array[Double] = classifier.distributionForInstance(inst)
      val cvalue = res.max
      val classid = classifier.m_structure.classAttribute().value(res.indexOf(cvalue))
      (r._2._2,classid,cvalue,r._1)
    })

    val writer = new PrintWriter(new File("test.txt"))
    cvData.foreach(r=>{
      writer.write(Array(r._1,r._2,r._3.toString,r._4).mkString("\t") + "\n")
    })
    writer.close()
  }

  def tryOpenFile(filename: String): Array[String] = {
    try {
      Source.fromFile(filename, "utf8").getLines().toArray
    } catch {
      case e: MalformedInputException => Source.fromFile(filename, "cp936").getLines().toArray
    }
  }

  def prepareTrainData(fname: String): Array[(String, (Array[String], String))] = {
    val stopWords = Array("的", "很", "啊", "手机", "吧", "呀", "了")
    tryOpenFile(fname).filter(_.length > 0).map(r => {
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
      (r._1, (words, r._2))
    }).filter(_._2._1.length > 0)
  }
}

class DMNBTextClassifierOpt(iterCount:Int) extends Serializable {
  //data structure
  var m_structure: Instances = null
  val m_indexDict: HMap[String, Attribute] = new HMap[String, Attribute]()
  var m_indexWord: HMap[String, Int] = new HMap[String, Int]()
  var m_dmnbClassifier = new DMNBtext

  def distributionForInstance(inst: Instance): Array[Double] = {
    m_dmnbClassifier.distributionForInstance(inst)
  }

  def trainClassifier(trainData: Array[(Array[String], String)]) {
    createStructure(trainData)
    val instances: Instances = new Instances(m_structure)
    //println(instances)
    var i = 0
    trainData.foreach(r => {
      instances.add(createSparseInstance(r._1,r._2,false))
      i += 1
      if (i % 100 == 0) println("creatInstance:" + i)
    })
    println("start building...")
    m_dmnbClassifier.setNumIterations(iterCount) //设定迭代次数
    m_dmnbClassifier.buildClassifier(instances)
    println("complete build!!")
  }

  // instances attribute
  def createStructure(data: Array[(Array[String], String)]) {
    val wordmap: Array[String] = data.flatMap(_._1).distinct
    val attributes: FastVector = new FastVector(wordmap.length)
    wordmap.foreach(r => {
      attributes.addElement(new Attribute(r))
    })

    //class value
    val classIDs = data.map(r => {
      r._2
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

  // 预测用样例,没有类别
  def createSparseInstance(wordlist: Array[String], needCount: Boolean): Instance = {
    val numAttr = m_structure.numAttributes()
    val attrValues: Array[Double] = Array.fill(numAttr)(0.0)
    val wordMap: HMap[String, Int] = new HMap[String, Int]()
    wordlist.filter(r => {
      m_indexWord.contains(r)
    }).foreach(r => {
      wordMap(r) = if (wordMap.contains(r)) 1 + wordMap(r) else 1
    })

    wordMap.foreach(w => {
      if (needCount) {
        attrValues(m_indexWord(w._1)) = w._2.toDouble
      } else {
        attrValues(m_indexWord(w._1)) = 1.0
      }
    })
    new SparseInstance(1, attrValues)
  }

  // 训练用样例,有类别
  def createSparseInstance(wordlist: Array[String], classid: String, needCount: Boolean): Instance = {
    val inst = createSparseInstance(wordlist, needCount)
    inst.setValue(m_structure.classAttribute(), classid)
    inst
  }
}
