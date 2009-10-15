package com.tfitter.trends

import java.util.BitSet
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import scala.collection.mutable.{Map,Set}

import org.suffix.util.JavaScalaIter._
import org.suffix.util.bdb.{BdbFlags, BdbArgs}

import com.tfitter.db.types._
import com.tfitter.db.Twit

import com.tfitter.corpus.{TVisitor,TwitterCorpus,ActOften}
import com.tfitter.corpus.RichTokenizedLM._
import System.err

object types {
type Word = String
type People = Set[UserID]
// day of year from 0 to Total_days in study
type Days = BitSet
}
import types._

case class WordInfo(
  word: String, // reduntant, "just in case"

// startTime: DateTime,
// endTime: DateTime,
// days: BitSet

  nTimes: Long,
  nTwits: Long,
  nReplies: Long,
  nInits: Long,

  people: People

// repliers: People,
// initiators: People
  )
  
class TVisitorWords(
	val lowerCase: Boolean,
	val twitProgress: Option[Long],
	val wordProgress: Option[Long], 
	val userProgress: Option[Long],
	val pairProgress: Option[Long]) extends TVisitor {
	
  var everySoOften: List[ActOften] = List()
	
  val tokenizerFactory = twitTokenizerFactory(lowerCase)
  // Map[Word,WordInfo]
  val words: Map[Word,People] = Map.empty
  val users: People = Set.empty // Set()
  var nWords = 0 // == words.size
  var nUsers = 0 // == users.size
  var nWordUsers = 0
  def doTwit(t: Twit) = {
    val tzer = tokenizerFactory.tokenizer(t.text.toArray,0,t.text.length) 
    val jter = tzer.iterator // Java iterator
    for (token <- jter) {
    	(words get token) match {
    		case Some(set) => // set += t.uid
    			// TODO demand notContains in 2.8!  a lá nonEmpty
    			if (!(set contains t.uid)) { 
					set += t.uid
    				nWordUsers += 1
    				pairProgress match {
    					case Some(n) if nWordUsers % n == 0 => print(":")
    					case _ =>
    				}
    				if (!(users contains t.uid)) {
    					users += t.uid
    					nUsers += 1
    					userProgress match {
    						case Some(n) if nUsers % n == 0 => print("o")
    						case _ =>
    					}
    				}
    			}
    		case _ => 
    			words(token) = Set(t.uid)
    			nWords += 1
    			wordProgress match {
    				case Some(n) if nWords % n == 0 => print("x")
    				case _ =>
    			}
    	}
    }
  }
}


object WordUsers extends optional.Application {

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
    lowerCase: Option[Boolean],
    
    showProgress: Option[Long],
    wordProgress: Option[Long],
    userProgress: Option[Long],
    pairProgress: Option[Long],
    args: Array[String]) = {
    
    val lowerCase_   = lowerCase getOrElse false

    val bdbEnvPath   = envName   getOrElse "bdb"
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
          
    val tv = new TVisitorWords(
      lowerCase_,
      showProgress,
      wordProgress,
      userProgress,
      pairProgress)
    
    maxTwits match {
      case Some(atMost) =>
        err.println("doing at most "+atMost+" tweets")
        twitCorpus.visitTake(atMost)(tv)
      case _ => 
        err.println("doing ALL tweets -- may take a LONG time!")
        twitCorpus.visitAll(tv)
    }
    
    err.println("the final stats of word/user-sets")
    err.println(tv.nWords+" words, "+tv.nUsers+" users, "+tv.nWordUsers+" total pairs.")
  }
}
