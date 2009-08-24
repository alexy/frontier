package com.tfitter.corpus

import System.err
// import com.tfitter.corpus.{TwitterCorpus,LM}
import com.tfitter.db.types._
import org.suffix.util.bdb.{BdbFlags, BdbArgs}
import com.tfitter.db.graph.Communities.UserList

import com.aliasi.tokenizer.{IndoEuropeanTokenizerFactory,TokenizerFactory}
import com.aliasi.lm.TokenizedLM
import com.aliasi.util.{Files,ScoredObject,AbstractExternalizable}

import java.io.{File,IOException}

import java.util.SortedSet
import scala.collection.jcl.Conversions._

case class SipParams (
  skipBack: Boolean,
  maxTwits: Option[Long],
  gram: Int,
  topGram: Int,
  top: Int,
  minCount: Int,
  pruneCount: Int,
  pruneOften: Option[Long],
  lowerCase: Boolean,
  caps: Boolean,
  showProgress: Option[Long],
  showOften: Option[Long]
)

object Sips extends optional.Application {  

  def main (
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    skipBack: Option[Boolean],
    maxTwits: Option[Long],
    showProgress: Option[Long],
    gram: Option[Int],
    topGram: Option[Int],
    minCount: Option[Int],
    top: Option[Int],
    pruneCount: Option[Int],
    pruneOften: Option[Long],
    lowerCase: Option[Boolean],
    caps: Option[Boolean],
    showOften: Option[Long],
    args: Array[String]) = {
      
    val NGRAM = 3
    val MIN_COUNT = 5
    val MAX_NGRAM_REPORTING_LENGTH = 2
    val NGRAM_REPORTING_LENGTH = 2
    val MAX_COUNT = 100
    val PRUNE_COUNT = 3

    val skipBack_ = skipBack getOrElse false
    val gram_ = gram getOrElse NGRAM
    val topGram_ = topGram getOrElse MAX_NGRAM_REPORTING_LENGTH
    val top_ = top getOrElse MAX_COUNT
    val minCount_ = minCount getOrElse MIN_COUNT
    val pruneCount_ = pruneCount getOrElse PRUNE_COUNT
    val lowerCase_ = !lowerCase.isEmpty
    val caps_ = !caps.isEmpty
    
    // TODO specify showProgress as 1/10 of maxTwits
    
    val sipParams = SipParams(
      skipBack_,
      maxTwits, // direct Option
      gram_,
      topGram_,
      top_,
      minCount_,
      pruneCount_,
      pruneOften, // direct Option
      lowerCase_,
      caps_,
      showProgress, // direct Option
      showOften // direct Option
      )
    
    val bdbEnvPath   = envName getOrElse "bdb"
    val bdbStoreName = storeName getOrElse "twitter"

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
    
    val cs = new ComSips(twitCorpus, sipParams)

    val com1: UserList = List(26329449,19032533,30931850,20798965,21779151,33616457,30259793,29667795,35202624,24393120,57118574,28838048,31437806,25355601,24761719,48897762,25688017,28361333,22382695,18877267,14803701,14742479)
   
    cs.comSips(com1)
  }
}

class ComSips(twitCorpus: TwitterCorpus, sp: SipParams) {
    type ScoredStrings = List[ScoredObject[Array[String]]]
  
    val nGramCount = NGramCount(sp.topGram,sp.top)
  
    if (!sp.showProgress.isEmpty) twitCorpus.setTwitsProgress(sp.showProgress.get)
    // twitCorpus.setMaxTwits(maxTwits getOrElse 1000000)
    twitCorpus.showNGrams = sp.showOften match {
      case Some(often) => Some(ShowTopNGrams(often,NGramCount(sp.topGram,sp.top)))
      case _ => None
    }
    
    val tf: TokenizerFactory = LM.twitTokenizerFactory(sp.lowerCase)
  
    val bgOpt: Option[TokenizedLM] = 
    if (sp.skipBack) None 
    else {
    	err.println("Training background model")
      val backgroundModel = new TokenizedLM(tf,sp.gram)
    
      sp.maxTwits match {
        case Some(atMost) =>
          err.println("doing at most "+atMost+" tweets")
          twitCorpus.visitTake(atMost)(backgroundModel)
        case _ => twitCorpus.visitAll(backgroundModel)
      }

      // can do every pruneChunk twits 
  	  backgroundModel.sequenceCounter.prune(sp.pruneCount)

      err.println("\nAssembling collocations in Training")
      val bgColls: ScoredStrings
        = backgroundModel.collocationSet(sp.topGram,
                                         sp.minCount,sp.top).toList

      println("\nBackground Collocations in Order of Significance:")
      report(bgColls,sp.caps)
      Some(backgroundModel)
    }
  
    def comSips(com: UserList) = {
      err.println("Training foreground model")
      val foregroundModel = new TokenizedLM(tf,sp.gram)
      twitCorpus.visitUsersLm(com, foregroundModel)
      foregroundModel.sequenceCounter.prune(sp.pruneCount)

      err.println("\nAssembling collocations in Test")
      val fgColls: ScoredStrings
        = foregroundModel.collocationSet(sp.topGram,
                                         sp.minCount,sp.top).toList

      println("\nForeground Collocations in Order of Significance:")
      report(fgColls,sp.caps)
    
      bgOpt match {
        case Some(bg) =>
          err.println("\nAssembling New Terms in Test vs. Training")
          val newTerms 
            = foregroundModel.newTermSet(sp.topGram,
      			       sp.minCount, sp.top,
      			       bg).toList
          err.println("\nNew Terms in Order of Signficance:")
          report(newTerms,sp.caps)
          err.println("\nDone.")
        case None =>
      }
    }
  
    def report(nGrams: ScoredStrings, caps: Boolean): Unit = {
    nGrams foreach { nGram =>
	    val score: Double = nGram.score
	    val toks: List[String] = nGram.getObject.toList
	    if (caps) report_caps(score,toks)
      else report_all(score,toks)
    }
  }
  
  def report_all(score: Double, toks: List[String]) = 
    println("Score: %5.3e with : %s" format (score, toks.mkString(" ")))
    
  def report_caps(score: Double, toks: List[String]) /* :Unit */ {
    if (toks forall capWord) report_all(score, toks)
	  else ()
  }

  def capWord(s: String) = s.toList match {
    case h :: t if (h.isUpperCase) && (t forall (_.isLowerCase)) => true
    case _ => false
  }

  // def capWord1(s: String) = !s.isEmpty && s.first.isUpperCase && (s.drop(1) forall (_.isLowerCase))
}
