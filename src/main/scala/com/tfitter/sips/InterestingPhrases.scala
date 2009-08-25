package com.tfitter.corpus

import System.err
import org.suffix.util.Info.info
import org.suffix.util.Debug
import org.suffix.util.input.Ints

// we *are* in .corpus, Toto:
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

// need to separate into its own file if more
// com.tfitter.corpus types are needed from other files
  
object types {
  type ScoredPhrases = List[ScoredObject[Array[String]]]
}
import types._

case class CollocationsMaybeSips (
  cols: ScoredPhrases,
  sips: Option[ScoredPhrases]
  )

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
    pruneCount: Option[Int],
    pruneOften: Option[Long],
    showProgress: Option[Long],
    gram: Option[Int],
    topGram: Option[Int],
    minCount: Option[Int],
    top: Option[Int],
    lowerCase: Option[Boolean],
    caps: Option[Boolean],
    sipsOnly: Option[Boolean],
    showOften: Option[Long],
    errSips: Option[Boolean],
    users: Option[String],
    debug: Option[Boolean],
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
    val sipsOnly_ = !skipBack_ && !sipsOnly.isEmpty
    val errSips_ = errSips getOrElse false
    val debug_ = debug getOrElse false
    
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

    val twitCorpus = new TwitterCorpus(bdbArgs, debug_)
    
    val cs = new ComSips(twitCorpus, sipParams)

    val userLists: List[UserList] =
    if (!users.isEmpty) {
      err.println("# for user list: "+users.get)
      List(Ints.getList(users.get))
    } else if (args.length < 1) {
      List[UserList](List(
          26329449,19032533,30931850,20798965,21779151,
          33616457,30259793,29667795,35202624,24393120,
          57118574,28838048,31437806,25355601,24761719,
          48897762,25688017,28361333,22382695,18877267,
          14803701,14742479))
    } else {
      val userListsFile = args(0)
      err.println("# reading userLists from "+userListsFile)
      Ints.readLists(userListsFile) 
    }
   
    userLists foreach { userList =>
      info("# community:")
      info(userList)
      val phrases = cs.comSips(userList,errSips_)
      if (!sipsOnly_) {
        info("# foreground collocations:")
        cs.report(phrases.cols,caps_)
      }
      phrases.sips match {
        case Some(sips) => 
          info("# foreground SIPs:")
          cs.report(sips,caps_)
        case _ =>
      }
    }
  }
}

class ComSips(twitCorpus: TwitterCorpus, sp: SipParams) {
  
    val nGramCount = NGramCount(sp.topGram,sp.top)
  
    if (!sp.showProgress.isEmpty) twitCorpus.setTwitsProgress(sp.showProgress.get)
    // twitCorpus.setMaxTwits(maxTwits getOrElse 1000000)
    twitCorpus.showNGrams = sp.showOften match {
      case Some(often) => Some(ShowTopNGrams(often,NGramCount(sp.topGram,sp.top)))
      case _ => None
    }
    
    val tf: TokenizerFactory = LM.twitTokenizerFactory(sp.lowerCase)
  
    val (bgOpt, bgColsOpt) = 
      if (sp.skipBack) (None, None) 
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
      val bgColls: ScoredPhrases
        = backgroundModel.collocationSet(sp.topGram,
                                         sp.minCount,sp.top).toList

      println("\nBackground Collocations in Order of Significance:")
      report(bgColls,sp.caps)
      
      (Some(backgroundModel), Some(bgColls))
    }
      
    def comSips(com: UserList, show: Boolean): CollocationsMaybeSips = {
      val dinfo = Debug.println(show)(_)
      
      dinfo("Training foreground model")
      val foregroundModel = new TokenizedLM(tf,sp.gram)
      twitCorpus.visitUsersLm(com, foregroundModel)
      foregroundModel.sequenceCounter.prune(sp.pruneCount)

      dinfo("\nAssembling collocations in Test")
      val fgColls: ScoredPhrases
        = foregroundModel.collocationSet(sp.topGram,
                                         sp.minCount,sp.top).toList

      dinfo("\nForeground Collocations in Order of Significance:")
      if (show) report(fgColls,sp.caps)
    
      val sips: Option[ScoredPhrases] = bgOpt match {
        case Some(bg) =>
          dinfo("\nAssembling New Terms in Test vs. Training")
          val newTerms 
            = foregroundModel.newTermSet(sp.topGram,
      			       sp.minCount, sp.top,
      			       bg).toList
          dinfo("\nNew Terms in Order of Signficance:")
          if (show) report(newTerms,sp.caps)
          dinfo("\nDone.")
          Some(newTerms)
        case _ => None
      }
      CollocationsMaybeSips(fgColls,sips)
    }
  
    def report(nGrams: ScoredPhrases, caps: Boolean): Unit = {
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
