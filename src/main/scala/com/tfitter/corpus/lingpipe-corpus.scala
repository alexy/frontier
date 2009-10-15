package com.tfitter.corpus

import com.aliasi.tokenizer._
import java.util.regex.Pattern
import com.aliasi.util.ScoredObject
import System.err
import com.aliasi.corpus.{TextHandler,Corpus}
// braver placed TokenNGramFiles in com.alias.lm from demo
import com.aliasi.lm.{TokenizedLM,TokenNGramFiles}

import com.tfitter.db.{Twit, TwitterDB, TwitterBDB, TwIterator}
import com.tfitter.db.types._
import org.suffix.util.bdb.{BdbFlags, BdbArgs}
import org.suffix.util.Info
import org.suffix.util.Info.info

import java.io.File

// java.util.SortedSet.toList
import scala.collection.jcl.Conversions._


case class NGramCount (
  nGram: Int,
  count: Int) {
  override def toString = {
    count+" top "+nGram+"-grams"
  }
}

case class CountOften(
  often: Long,
  count: Int)
  
case class ActOften(
  often:  Long,
  action: Long => Unit)


abstract class TVisitor {
  val twitProgress: Option[Long]
  var everySoOften: List[ActOften]
  def doTwit(t: Twit): Unit
}

class TwitterCorpus(bdbArgs: BdbArgs) {
  
  def visit(twits: Iterator[Twit])(tv: TVisitor): Unit = {
    if (twits.hasNext) info('gottwits)("got twits") //'
    var twitCount: Long = 0
    for (t <- twits) {
      tv.doTwit(t)
      twitCount += 1
      tv.twitProgress match {
        case Some(x) if (twitCount % x == 0) => err.print(".")
        case _ =>
      }
      // TODO show before or after pruning?
      tv.everySoOften foreach { e =>
        if (twits.hasNext && twitCount % e.often == 0)
          e.action(twitCount)
      }
    }
    info('twitwalk)("did "+twitCount+" twits.") //'
  }
  
  val visitAll = visit(tdb.allTwits)(_)

  // TODO until stdlib Iterator's take's param is Int, not Long, have to toInt!
  def visitTake(atMost: Long)(tv: TVisitor) = visit(tdb.allTwits.take(atMost.toInt))(tv)
  
  def visitUsers(users: List[UserID], tv: TVisitor) = 
    users foreach { uid =>
      visit(tdb.userTwits(uid))(tv)
    }

  val tdb: TwitterDB = new TwitterBDB(bdbArgs)
}
  
  
class RichTokenizedLM(tokenizerFactory: TokenizerFactory, nGram: Int) 
  extends TokenizedLM(tokenizerFactory,nGram) {
  def showTopNGrams(nGramCount: NGramCount): Unit = {
    val NGramCount(topGram,topCount) = nGramCount

    val freqTerms: List[ScoredObject[Array[String]]] = frequentTermSet(topGram, topCount).toList

    for (so <- freqTerms) {
      println(so.score+": "+so.getObject.toList.mkString(","))
    }
  }

}


object RichTokenizedLM {
  // TODO we need some other way to pimp it up from just an lm parameter:
  // implicit def richTokenizedLM(lm: TokenizedLM): RichTokenizedLM = new RichTokenizedLM(lm)

  // TODO add @usernames and #hashtags
  def twitTokenizerFactory(lowerCase: Boolean): TokenizerFactory = {
    var factory = IndoEuropeanTokenizerFactory.INSTANCE
    factory = new RegExFilteredTokenizerFactory(factory,Pattern.compile("\\p{Alpha}+"))
    if (lowerCase) factory = new LowerCaseTokenizerFactory(factory)
    factory = new EnglishStopTokenizerFactory(factory)
    factory
  }
}


class TVisitorLM(
  val nGram: Int,
  val lowerCase: Boolean,
  val nGramCount: NGramCount, 
  val dumpOften: Option[Long], 
  val pruneOften: Option[CountOften],
  val twitProgress: Option[Long]) extends TVisitor {
    
  // no need for pimping -- lm is created explicitly rich   
  // import RichTokenizedLM._
    
  // no need to have parts at the top
  // val NGramCount(topGram,topCount) = nGramCount
  
  var dumpCount = 0
  
  def reset: Unit = {
    dumpCount = 0
  }
  
  def dumpNGrams(twitCount: Long): Unit = {
    dumpCount += 1
    info('dumpngrams)("#"+dumpCount+"("+twitCount+" twits) intermediate dump of "+nGramCount) //'
    showTopNGrams(nGramCount)
  }

  def prune(twitCount: Long) = {
    pruneOften match {
      case Some(o) =>
        info('prune)("pruning lm after "+twitCount+" twits at mincount\n => before: "+lm.sequenceCounter.trieSize+" ngrams, after: ") //'
        lm.sequenceCounter.prune(o.count)
        info('prune)(lm.sequenceCounter.trieSize) //'
      case _ =>
    }
  }
  
  // we need LM.showTopNGrams to be used with ReadNGrams
  def showTopNGrams(nGramCount: NGramCount): Unit = lm.showTopNGrams(nGramCount)
  
  val tokenFactory: TokenizerFactory = RichTokenizedLM.twitTokenizerFactory(lowerCase)
  val lm = new RichTokenizedLM(tokenFactory,nGram)
  
  def doTwit(twit: Twit) = lm.train(twit.text.toCharArray,0,twit.text.length)

  var everySoOften: List[ActOften] = List()  
  dumpOften match {
    case Some(often) => 
      everySoOften ::= ActOften(often, dumpNGrams)
    case _ =>
  }
  pruneOften match {
    case Some(o) => 
      everySoOften ::= ActOften(o.often, prune)
    case _ =>
  }
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
    dumpOften: Option[Long],
    debugOn: Option[String],
    debugOff: Option[String],
    groupOn: Option[String],
    groupOff: Option[String],
    write: Option[String],
    args: Array[String]) = {

    val gram_ = gram getOrElse 2
    val lowerCase_ = !lowerCase.isEmpty
    val topGram_ = topGram getOrElse gram_
    val top_ = top getOrElse 20
    val nGramCount = NGramCount(topGram_,top_)
    val pruneCount_ = pruneCount getOrElse 2
    val pruneOftenCount = pruneOften match {
      case Some(often) => 
        Some(CountOften(often,pruneCount_))
      case _ => None
    }

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
          
    val twitVisitor = new TVisitorLM(
      gram_,lowerCase_,
      nGramCount,dumpOften,
      pruneOftenCount,
      showProgress)
    
    maxTwits match {
      case Some(atMost) =>
        err.println("doing at most "+atMost+" tweets")
        twitCorpus.visitTake(atMost)(twitVisitor)
      case _ => 
        err.println("doing ALL tweets -- may take a LONG time!")
        twitCorpus.visitAll(twitVisitor)
    }
    
    err.println("the final dump of "+nGramCount)    
    twitVisitor.showTopNGrams(nGramCount)
  
    val minWriteOrder = 0
    val maxWriteOrder = gram_
    val minWriteCount = 1
    write match {
      case Some(file) => 
           err.println("serializing the ngram model into "+file+
           ", minOrder="+minWriteOrder+", maxOrder="+maxWriteOrder+
           ", minCount="+minWriteCount)
           TokenNGramFiles.writeNGrams(twitVisitor.lm,new File(file),
           minWriteOrder,
           maxWriteOrder,
           minWriteCount,
           "UTF-8")
      case _ =>
    } 
  }
}


object ReadNGrams extends optional.Application {
  def main(gram: Option[Int],
           topGram: Option[Int],
           top: Option[Int],
           lowerCase: Option[Boolean],
           ngrams: String) {
             
        val gram_ = gram getOrElse 3
        val topGram_ = topGram getOrElse gram_
        val top_ = top getOrElse 20
        val lowerCase_ = lowerCase getOrElse false
        
        val nGramCount = NGramCount(topGram_,top_)
        
        // read merged LM and write
        val tokenizerFactory = LM.twitTokenizerFactory(lowerCase_)
        val lm = new RichTokenizedLM(tokenizerFactory, gram_)
        
        TokenNGramFiles.addNGrams(new File(ngrams),"UTF-8",lm,0)
        err.println("finished reading back the model, finding "+ top_ +" top "+ topGram_ +"-grams...")
        lm.showTopNGrams(nGramCount)
  }
}