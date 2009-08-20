package com.tfitter.corpus

import System.err
// import com.tfitter.corpus.{TwitterCorpus,LM}
import com.tfitter.db.types._
import org.suffix.util.bdb.{BdbFlags, BdbArgs}

import com.aliasi.tokenizer.{IndoEuropeanTokenizerFactory,TokenizerFactory}
import com.aliasi.lm.TokenizedLM
import com.aliasi.util.{Files,ScoredObject,AbstractExternalizable}

import java.io.{File,IOException}

import java.util.SortedSet
import scala.collection.jcl.Conversions._

object Sips extends optional.Application {  
  type ScoredStrings = List[ScoredObject[Array[String]]]

  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    maxTwits: Option[Int],
    showProgress: Option[Int],
    gram: Option[Int],
    topGram: Option[Int],
    minCount: Option[Int],
    top: Option[Int],
    showOften: Option[Long],
    args: Array[String]) = {
      
    val comm1: List[UserID] = List(26329449,19032533,30931850,20798965,21779151,33616457,30259793,29667795,35202624,24393120,57118574,28838048,31437806,25355601,24761719,48897762,25688017,28361333,22382695,18877267,14803701,14742479)

    val NGRAM = 3
    val MIN_COUNT = 5
    val MAX_NGRAM_REPORTING_LENGTH = 2
    val NGRAM_REPORTING_LENGTH = 2
    val MAX_COUNT = 100
    val PRUNE_COUNT = 3

    val gram_ = gram getOrElse NGRAM
    val topGram_ = topGram getOrElse MAX_NGRAM_REPORTING_LENGTH
    val minCount_ = minCount getOrElse MIN_COUNT
    val top_ = top getOrElse MAX_COUNT
    
    val nGramCount = NGramCount(topGram_,top_)

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
    
    if (!showProgress.isEmpty) twitCorpus.setTwitsProgress(showProgress.get)
    // twitCorpus.setMaxTwits(maxTwits getOrElse 1000000)
    twitCorpus.showNGrams = showOften match {
      case Some(often) => Some(ShowTopNGrams(often,NGramCount(topGram_,top_)))
      case _ => None
    }
    
	  err.println("Training background model")
    val tf: TokenizerFactory = LM.twitTokenizerFactory
    val backgroundModel = new TokenizedLM(tf,gram_)

    maxTwits match {
      case Some(atMost) =>
        err.println("doing at most "+atMost+" tweets")
        twitCorpus.visitTake(atMost)(backgroundModel)
      case _ => twitCorpus.visitAll(backgroundModel)
    }

    // can do every pruneChunk twits 
	  backgroundModel.sequenceCounter.prune(PRUNE_COUNT)

    err.println("\nAssembling collocations in Training")
    val coll: ScoredStrings
      = backgroundModel.collocationSet(topGram_,
                                       minCount_,top_).toList

    println("\nCollocations in Order of Significance:")
    report(coll)

    err.println("Training foreground model")
    val foregroundModel = new TokenizedLM(tf,gram_)
    twitCorpus.visitUsersLm(comm1, foregroundModel)
    foregroundModel.sequenceCounter.prune(PRUNE_COUNT)

    err.println("\nAssembling New Terms in Test vs. Training")
    val newTerms 
      = foregroundModel.newTermSet(topGram_,
			       minCount_, top_,
			       backgroundModel).toList

    err.println("\nNew Terms in Order of Signficance:")
    report(newTerms)
	
    err.println("\nDone.")
  }

  def report(nGrams: ScoredStrings): Unit = {
    nGrams foreach { nGram =>
	    val score: Double = nGram.score
	    val toks: List[String] = nGram.getObject.toList
	    // TODO optionalize filtering by capWord or some such:
	    // report_filter(score,toks)
      report_all(score,toks)
    }
  }
  
  def report_all(score: Double, toks: List[String]) = 
    println("Score: %5.3e with : %s" format (score, toks.mkString(" ")))
    
  def report_filter(score: Double, toks: List[String]) /* :Unit */ {
    if (toks forall capWord)
	    println("Score: %5.3e with : %s" format (score, toks.mkString(" ")))
	  else ()
  }

  def capWord(s: String) = s.toList match {
    case h :: t if (h.isUpperCase) && (t forall (_.isLowerCase)) => true
    case _ => false
  }

  def capWord1(s: String) = !s.isEmpty && s.first.isUpperCase && (s.drop(1) forall (_.isLowerCase))
}
