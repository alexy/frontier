package com.tfitter.corpus

import System.err
import org.suffix.util.Info
import org.suffix.util.Info.{say,info}
import org.suffix.util.Debug
import org.suffix.util.input.Ints

// we *are* in .corpus, Toto:
// import com.tfitter.corpus.{TwitterCorpus,LM}
import com.tfitter.db.types._
import org.suffix.util.bdb.{BdbFlags,BdbArgs,BdbStore}
import com.tfitter.db.graph.{RepMapsBDB,Communities}
import com.tfitter.db.graph.Communities.{UserList,Community}

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
    dataDir: Option[String],
    twitsEnvName: Option[String],
    twitsStoreName: Option[String],
    twitsCacheSize: Option[Double],
    repsEnvName: Option[String],
    repsStoreName: Option[String],
    repsCacheSize: Option[Double],
    
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
    errSips: Option[Boolean], // output Sips to stderr in addition to API return
    users: Option[String],
    
    pairs: Option[String],
    csv: Option[Boolean],
    maxMembers: Option[Int],
    maxGen: Option[Int],
    asym: Option[Boolean], // asymmetric communities    
    args: Array[String]) = {
      
    val NGRAM = 3
    val MIN_COUNT = 5
    val MAX_NGRAM_REPORTING_LENGTH = 2
    val NGRAM_REPORTING_LENGTH = 2
    val MAX_COUNT = 100
    val PRUNE_COUNT = 3

    val skipBack_ = skipBack getOrElse false
    val gram_ = gram getOrElse NGRAM
    val topGram_ = topGram getOrElse gram_ //MAX_NGRAM_REPORTING_LENGTH
    val top_ = top getOrElse MAX_COUNT
    val minCount_ = minCount getOrElse MIN_COUNT
    val pruneCount_ = pruneCount getOrElse PRUNE_COUNT
    val lowerCase_ = !lowerCase.isEmpty
    val caps_ = !caps.isEmpty
    val sipsOnly_ = !skipBack_ && !sipsOnly.isEmpty
    val errSips_ = errSips getOrElse false
    val asym_ = asym getOrElse false
    
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
      
    List('bgmodel,'maxtwits,'fg) foreach Info.set //'
    
    val twitsBdbEnvPath   = twitsEnvName   getOrElse "twits.bdb"
    val twitsBdbStoreName = twitsStoreName getOrElse "twitter"
    val twitsBdbCacheSize = BdbStore.cacheSizeBytesOpt(twitsCacheSize)

    val bdbReadFlags = BdbFlags(
      /* allowCreate */    false,
      /* readOnly */       true,
      /* transactional */  false,
      /* deferredWrite */  false,
      /* noSync */         false
    )
    
    val twitsBdbArgs = BdbArgs(twitsBdbEnvPath,twitsBdbStoreName,bdbReadFlags,twitsBdbCacheSize)
    val twitCorpus = new TwitterCorpus(twitsBdbArgs)
    val cs = new ComSips(twitCorpus, sipParams)
    
    // grow communities from seeding pairs, if requested
    val bdbDataDir = dataDir getOrElse "" // Config.bdbDataDir
    val growComs = !pairs.isEmpty
    
    // TODO use file-system-dependent path concat:
    val repsBdbEnvPath   = repsEnvName   getOrElse (bdbDataDir + "urs.bdb")
    val repsBdbStoreName = repsStoreName getOrElse "repmaps"   
    val repsBdbCacheSize = BdbStore.cacheSizeBytesOpt(repsCacheSize)
    
    val repsBdbArgs = BdbArgs(repsBdbEnvPath,repsBdbStoreName,bdbReadFlags,repsBdbCacheSize)
    val udb = new RepMapsBDB(repsBdbArgs)
    val coms = new Communities(udb.getReps _)


    val userLists: List[UserList] =
    if (!users.isEmpty) {
      err.println("# for user list: "+users.get)
      List(Ints.getList(users.get))
    } else if (!pairs.isEmpty) {
      val pairs_ = Ints.getPairs(pairs.get)
      pairs_ map { pair =>
        val com: Community = coms.triangles(pair,!asym_,maxMembers,maxGen)
        com map { _._1 }
        }
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
      info('sipscom)("# community:") //'
      info('sipscom)(userList) //'
      val phrases = cs.comSips(userList,errSips_)
      if (!sipsOnly_) {
        println("# foreground collocations:")
        cs.report(phrases.cols,caps_)
      }
      phrases.sips match {
        case Some(sips) => 
          println("# foreground SIPs:")
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
      	info('bgmodel)("Training background model") //'
        val backgroundModel = new TokenizedLM(tf,sp.gram)
    
        sp.maxTwits match {
          case Some(atMost) =>
            info('maxtwits)("doing at most "+atMost+" tweets") //'
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

      info('fg)("\nAssembling collocations in Test") //'
      val fgColls: ScoredPhrases
        = foregroundModel.collocationSet(sp.topGram,
                                         sp.minCount,sp.top).toList

      info('fg)("\nForeground Collocations in Order of Significance:") //'
      if (show) report(fgColls,sp.caps)
    
      val sips: Option[ScoredPhrases] = bgOpt match {
        case Some(bg) =>
          info('fg)("\nAssembling New Terms in Test vs. Training") //'
          val newTerms 
            = foregroundModel.newTermSet(sp.topGram,
      			       sp.minCount, sp.top,
      			       bg).toList
          info('fg)("\nNew Terms in Order of Signficance:") //'
          if (show) report(newTerms,sp.caps)
          info('fg)("\nDone.") //'
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
