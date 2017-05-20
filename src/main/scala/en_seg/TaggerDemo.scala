package en_seg

import edu.stanford.nlp.ling.HasWord
import edu.stanford.nlp.ling.SentenceUtils
import edu.stanford.nlp.ling.TaggedWord
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import edu.stanford.nlp.util.logging.Redwood

import java.io.BufferedReader
import java.io.FileReader
import java.util.List

import scala.collection.JavaConversions._

object TaggerDemo {
  def main(args:Array[String]):Unit ={
    /*
    if (args.length != 2) {
      log.info("usage: java TaggerDemo modelFile fileToTag")

    }
    */

    val tagger:MaxentTagger  = new MaxentTagger(args(0))
    val sentences:List<List<HasWord>>= MaxentTagger.tokenizeText(new BufferedReader(new FileReader(args(1))))

    for (sentence:List<HasWord> <- sentences) {
      val tSentence:List<TaggedWord> = tagger.tagSentence(sentence)
      println(SentenceUtils.listToString(tSentence, false))
    }
  }

}
