

import scala.collection.convert.wrapAll._
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import java.util.Properties

object SentimentFinder {


  val property = new Properties()

  property.setProperty("annotators", "tokenize, ssplit, parse, sentiment")

  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(property)

  // gets data
  def sentiment(data: String): Int = Option(data) match {

    case Some(data) if data.nonEmpty  =>  fetchSents(data)

    case _  =>  throw new IllegalArgumentException("empty dat")

  }


  // fetches sentiments
private def fetchSents(words: String): Int = {

  val  ( _, sents)  =  getSents(words).maxBy  {  case  ( sent,  _ )  =>  sent.length  }

  sents

}

  // gets sentiments
  private def getSents(words: String): List[(String, Int)] = {

    val annotes: Annotation = pipeline.process(words)

    val data = annotes.get(classOf[CoreAnnotations.SentencesAnnotation])

    data.map( curr  =>  ( curr,  curr.get( classOf[SentimentCoreAnnotations.SentimentAnnotatedTree] ) ) ).map { case ( curr, tree ) => ( curr.toString, RNNCoreAnnotations.getPredictedClass( tree ) )  }.toList

  }

}






