package com.tfitter.influence

import scala.collection.mutable.Map

import com.tfitter.tokenizer._
import com.tfitter.corpus.{TwitterCorpus,TVisitor,ActOften}
import org.suffix.util.bdb.{BdbFlags, BdbArgs}
import System.err
import com.tfitter.db.{Twit}
import com.tfitter.db.types._


object RT {
  val prefix = Set("rt","via")
  def is(word: String): Boolean = {
    val wordLower = word.toLowerCase
    prefix contains wordLower
  }
}

case class UserRetwits(
  user:     String,
  numFans:  Int,
  retwits:  Int,
  topFan:   TwitID,
  topCount: Int)

// originally I wanted to have, for each auhor,
// a list of twits most retweeted, with counts;
// yet then I realized we don't have immediate original twit id (twid?)
// so I used the same logic to count top fans instead!

class Retwiters(maxShow: Int) {
  val data: Map[String,Map[UserID,Int]] = Map()

  def add(u: String, v: UserID) = {
     data.get(u) match {
       case Some(_) => data(u).get(v) match {
         case Some(_) => data(u)(v) += 1
         case _ => data(u)(v) = 1
       }
       case _ => data(u) = Map(v->1)
     }
  }

  def retwiters: Array[UserRetwits] = {
    val topList = 
    data.toList map { case (user,retwits) =>
      // for lists, we can init fold with head and feed with tail
      // TODO can something such be done for map -- via toList?
      val (topFanCount, totalRetwits) = retwits.foldLeft(((0,0),0)) {
        case ((keep @ (topFan,topCount), totalRetwits), replace @ (fan,count)) =>
        val fanCount: (UserID, Int) = if (topCount < count) replace else keep
        (fanCount,totalRetwits+count)
      }
      val (topFan, topCount) = topFanCount
      UserRetwits(user,retwits.size,totalRetwits,topFan,topCount)
    }
    topList.toArray sortWith (_.retwits > _.retwits)
  }

  override def toString = retwiters.take(maxShow).mkString("\n")
}

class InfluIndex(val twitProgress: Option[Long]) extends TVisitor {

	// val twits = Array[TwitHash]()

	var twitCount: Long = 0
	var everySoOften: List[ActOften] = List()
	var isRT = false
  val retwiters = new Retwiters(5)
  
	override def doTwit(t: Twit, tc: Long) = {
		twitCount = tc
	  val tzer = new TwitTokens(t.text)

		for (token <- tzer) {
      token match {
        case TwitTokenWord(w) => isRT = RT.is(w)
        case TwitTokenUser(u) if (isRT) => retwiters.add(u, t.uid)
        case _ =>
      }
		}	
	}
	
	override def toString: String = {
		"total twits in influindex: "+twitCount+"\n"+retwiters
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
    twitProgress: Option[Long]) = {
    
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
