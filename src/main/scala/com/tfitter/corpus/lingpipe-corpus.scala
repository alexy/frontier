package com.tfitter.corpus

import com.aliasi.tokenizer._
import java.util.regex.Pattern
import com.aliasi.util.ScoredObject
import System.err
import com.aliasi.corpus.{TextHandler,Corpus}
import com.aliasi.lm.TokenizedLM

import com.tfitter.db.{Twit, TwitterDB, TwitterBDB, TwIterator}
import com.tfitter.db.types._
import org.suffix.util.bdb.{BdbFlags, BdbArgs}
import org.suffix.util.Info
import org.suffix.util.Info.info

// java.util.SortedSet.toList
import scala.collection.jcl.Conversions._

case class NGramCount (
  nGram: Int,
  count: Int
)

case class ShowTopNGrams (
  often: Long,
  nGramCount: NGramCount
)

class TwitterCorpus(bdbArgs: BdbArgs) extends Corpus[TokenizedLM] {
  
  override def visitTest(lm: TokenizedLM): Unit = {}

  // Iterator.take(Int) -- not Long, unacceptable generally!
  private var maxTwits: Option[Long] = None
  def setMaxTwits(m: Long): Unit = maxTwits = Some(m)
  def unsetMaxTwits: Unit = maxTwits = None

  private var twitsProgress: Option[Long] = None
  def setTwitsProgress(m: Long): Unit = twitsProgress = Some(m)
  def unsetTwitsProgress: Unit = twitsProgress = None

  var showNGrams: Option[ShowTopNGrams] = None
  
  var pruneCount: Option[Int] = None
  var pruneOften: Option[Long] = None

  def visitLm(twits: Iterator[Twit])(lm: TokenizedLM): Unit = {
    val twitsToGo = maxTwits match {
          // m.toInt for stdlib sillily takes Int, not Long!
          case Some(m) => 
            info('maxtwits)("# respecting corpus maxTwits cutoff set at "+m) //'
            twits.take(m.toInt)
          case _ => twits
        }
    if (twitsToGo.hasNext) info('gottwits)("got twits") //'
    // get the total here as it's "remaining"!
    var twitCount: Long = 0
    var dumpCount: Int  = 0
    for (t <- twitsToGo) { // t <- twitsToGo
      // err.println(totalTwits+": "+t.text)
      // TODO lm.train is same as lm.handle?
      lm.train(t.text.toCharArray,0,t.text.length)
      twitCount += 1
      if (!twitsProgress.isEmpty && twitCount % twitsProgress.get == 0) err.print(".")
      // TODO show before or after pruning?
      showNGrams match {
        case Some(x) if (twitCount % x.often == 0) =>
          dumpCount += 1
          info('dumpngrams)("#"+dumpCount+" intermediate dump of "+LM.showNGramCount(x.nGramCount)) //'
          LM.showTopNGrams(lm,x.nGramCount)
        case _ =>
      }
      pruneOften match {
        case Some(often) if (!pruneCount.isEmpty && twitCount % often == 0) =>
          val minCount = pruneCount.get
          info('prune)("pruning lm after "+twitCount+" twits at mincount\n => before: "+lm.sequenceCounter.trieSize+" ngrams, after: ") //'
          lm.sequenceCounter.prune(minCount)
          info('prune)(lm.sequenceCounter.trieSize) //'
        case _ =>
      }
    }
    info('twitwalk)("did "+twitCount+" twits.") //'
  }
  
  val visitAll = visitLm(tdb.allTwits)(_)
  // need to override visitTrain, should be def not val:
  // val visitTrain = visitAll _
  override def visitTrain(lm: TokenizedLM): Unit = visitAll(lm)
  // until stdlib Iterator's take's param is Int, not Long, have to toInt!
  def visitTake(atMost: Long)(lm: TokenizedLM) = visitLm(tdb.allTwits.take(atMost.toInt))(lm)
  
  def visitUsersLm(users: List[UserID], lm: TokenizedLM) = 
    users foreach { uid =>
      visitLm(tdb.userTwits(uid))(lm)
    }

  val tdb: TwitterDB = new TwitterBDB(bdbArgs)
}

object TopNGrams extends optional.Application {

  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    maxTwits: Option[Long],
    pruneCount: Option[Int],
    pruneOften: Option[Long],
    showProgress: Option[Long],
    lowerCase: Option[Boolean],
    gram: Option[Int],
    topGram: Option[Int],
    top: Option[Int],
    showOften: Option[Long],
    debugOn: Option[String],
    debugOff: Option[String],
    groupOn: Option[String],
    groupOff: Option[String],
    args: Array[String]) = {

    val lowerCase_ = !lowerCase.isEmpty
    val gram_ = gram getOrElse 2
    val topGram_ = topGram getOrElse gram_
    val top_ = top getOrElse 20

    val nGramCount = NGramCount(topGram_,top_)

    val bdbEnvPath   = envName getOrElse "bdb"
    val bdbStoreName = storeName getOrElse "twitter"
    
    Info.group('G_twitwalk,List('twitwalk,'gottwits,'maxtwits))
    val showNot = Info.getSymbols(debugOff)
    val showOn = (debugOn match {
      case Some(s) => Info.symbols(s)
      case _ => List('maxtwits,'gottwits,'dumpngrams,'prune,'twitwalk)  //'
    }) remove (showNot contains _)
    
    showOn foreach Info.set
    info('info)("# showing output for symbols:"+showOn) //'
    
    val bdbCacheSize = cacheSize match {
      case Some(x) => Some((x*1024*1024*1024).toLong)
      case _ => None // Config.bdbCacheSize
    }
    val bdbFlags = BdbFlags(
      allowCreate   getOrElse false,
      readOnly      getOrElse true,
      transactional getOrElse false,
      deferredWrite getOrElse false,
      noSync        getOrElse false
    )
    val bdbArgs = BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)

    val twitCorpus = new TwitterCorpus(bdbArgs)
    
    if (!showProgress.isEmpty) twitCorpus.setTwitsProgress(showProgress.get)
    // twitCorpus.setMaxTwits(maxTwits getOrElse 100)

    twitCorpus.showNGrams = showOften match {
      case Some(often) => Some(ShowTopNGrams(often,NGramCount(topGram_,top_)))
      case _ => None
    }
    twitCorpus.pruneCount = pruneCount
    twitCorpus.pruneOften = pruneOften
    
    val tf: TokenizerFactory = LM.twitTokenizerFactory(lowerCase_)
    val lm = new TokenizedLM(tf,gram_)

    maxTwits match {
      case Some(atMost) =>
        err.println("doing at most "+atMost+" tweets")
        twitCorpus.visitTake(atMost)(lm)
      case _ => twitCorpus.visitAll(lm)
    }
    err.println("the final dump of "+LM.showNGramCount(nGramCount))    
    LM.showTopNGrams(lm,nGramCount)
  }

}

object LM {
  def showTopNGrams(lm: TokenizedLM, nGramCount: NGramCount) = {
    val NGramCount(nGram,count) = nGramCount
    val freqTerms: List[ScoredObject[Array[String]]] = lm.frequentTermSet(nGram, count).toList

    for (so <- freqTerms) {
      println(so.score+": "+so.getObject.toList.mkString(","))
    }
  }

  def twitTokenizerFactory(lowerCase: Boolean): TokenizerFactory = {
      var factory = IndoEuropeanTokenizerFactory.INSTANCE
      factory = new RegExFilteredTokenizerFactory(factory,Pattern.compile("\\p{Alpha}+"))
      if (lowerCase) factory = new LowerCaseTokenizerFactory(factory)
      factory = new EnglishStopTokenizerFactory(factory)
      factory
  }

  def showNGramCount(nc: NGramCount) = {
    nc.count+" top "+nc.nGram+"-grams"
  }
}