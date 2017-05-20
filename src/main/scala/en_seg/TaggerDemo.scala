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
    List<List<HasWord>> sentences = MaxentTagger.tokenizeText(new BufferedReader(new FileReader(args(1))))

    for (sentence:List<HasWord> <- sentences) {
      List<TaggedWord> tSentence = tagger.tagSentence(sentence);
      System.out.println(SentenceUtils.listToString(tSentence, false));
    }
  }

}
