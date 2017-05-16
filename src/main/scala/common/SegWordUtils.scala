package common

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.NotionalTokenizer

import scala.collection.JavaConversions._

/**
  * Created by duanxiping on 2017/2/5.
  */
object SegWordUtils {
  HanLP.Config.Normalization = true
  def addStopWords(words:Array[String]) = {
    words.foreach(r=>{
      CoreStopWordDictionary.add(r)
    })
  }

  def addUserDefineWords(words:Array[(String,String)]) ={
    words.foreach(r=>{
      CustomDictionary.add(r._1,r._2 + " 1024")
    })
  }

  def segMsg(msg: String): Array[String] = {
    HanLP.segment(msg).map(r => {
      r.word
    }).toArray
  }

  def segMsgWithNature(msg: String): Array[(String, String)] = {
    HanLP.segment(msg).map(r => {
      (r.word, r.nature.toString)
    }).toArray
  }
}
