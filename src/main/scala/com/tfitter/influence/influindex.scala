package com.tfitter.influence

import scala.collection.mutable.Map

import com.tfitter.tokenizer._
import com.tfitter.corpus.{TwitterCorpus,TVisitor,ActOften}
import org.suffix.util.bdb.{BdbFlags, BdbArgs}
import com.tfitter.db.Twit
import System.err

case class PosCount {
	pos:   Int,
	count: Int
}

object types {
	type WordPosCount = Map[String, PosCount] 
}

class WordHash {
	vol words: WordPosCount = Map()
	
	def addT(w: String, pos: Int) = {
		words.get(w) match {
			case Some(PosCount(pos,count) => words(w) = PosCount(pos,count+1)
			case _ => words(w) = PosCount(pos,1)
		}
	}
}

class TwitHash {
	val urls = new WordHash
	val users: WordHash
	// val retweets: WordHash
	val hashtags: WordHash
	val words: WordHash
	
	def addToken(token: TwitToken, pos: Int) = {
		token match {
			case TwitTokenUser(w) => users.addToken(w, pos)
			case TwitTokenTag(w)  => tags.addToken(w, pos)
			case TwitTokenUrl(w)  => urls.addToken(w, pos)
			case TwitTokenWord(w) => words.addToken(w, pos)
			case TwitTokenNone()  => // TODO shouldn't happen?
		}
	}
}

class InfluIndex(val twitProgress: Option[Long]) extends TVisitor {

	val twits = Array[TwitHash]()

	var twitCount: Long = 0
	var everySoOften: List[ActOften] = List()
	
	override def doTwit(t: Twit, tc: Long) = {
		twitCount = tc
	    val tzer = new TwitTokens(t.text)
		for (token <- tzer) {
		}	
	}
	
	override def toString: String = {
		"total twits in influindex: "+twitCount
	}
}


object Influence extends optional.Application {

  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],

    maxTwits:  Option[Long],
    twitProgress: Option[Long],
    
    args: Array[String]) = {
    
    def givenOrPooled(given: Option[Long], pool: Option[Long]): Option[Long] = 
    	if (given.isEmpty) pool
    	else given
		    
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
          
	val tv = new InfluIndex(twitProgress)
        
    err.println("the twit visitor has "+tv.everySoOften.size+" periodic actions:"+tv.everySoOften)
    
    maxTwits match {
      case Some(atMost) =>
        err.println("doing at most "+atMost+" tweets")
        twitCorpus.visitTake(atMost)(tv)
      case _ => 
        err.println("doing ALL tweets -- may take a LONG time!")
        twitCorpus.visitAll(tv)
    }
        	
    err.println("\nthe final stats of word/user-sets")
    err.println(tv)
    
  }
}
