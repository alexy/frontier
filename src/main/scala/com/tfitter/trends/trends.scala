package com.tfitter.trends

import java.util.BitSet
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import scala.collection.mutable.{Map,Set}

import org.suffix.util.bdb.{BdbFlags, BdbArgs}

import com.tfitter.db.types._
import com.tfitter.db.{Twit,TwitRole,TwitInit,TwitReply,TwitMumble}

import com.tfitter.tokenizer._
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


case class WordUserProgress(
	word: Option[Long],
	user: Option[Long],
	pair: Option[Long])
	
	
// NB could make WordUserProgress constructor argument
case class WordPeople(name: String) {
  val words: Map[Word,People] = Map.empty
  val users: People = Set.empty
  var nWords = 0 // == words.size
  var nUsers = 0 // == users.size
  var nWordUsers = 0

  def addUser(token: Word, user: UserID, progress: WordUserProgress): Unit = {
      	(words get token) match {
    		case Some(set) => // set += user
    			// TODO demand notContains in 2.8!  a lá nonEmpty
    			if (!(set contains user)) { 
					set += user
    				nWordUsers += 1
    				progress.pair match {
    					case Some(n) if nWordUsers % n == 0 => print(":")
    					case _ =>
    				}
    				if (!(users contains user)) {
    					users += user
    					nUsers += 1
    					progress.user match {
    						case Some(n) if nUsers % n == 0 => print("o")
    						case _ =>
    					}
    				}
    			}
    		case _ => 
    			words(token) = Set(user)
    			nWords += 1
    			progress.word match {
    				case Some(n) if nWords % n == 0 => print("x")
    				case _ =>
    			}
    	}
  }
  
  override def toString =
    name+":WordPeople: has "+nWords+" words, "+nUsers+" users, "+nWordUsers+" total pairs"
}


case class WordInfo(
	word: String, // reduntant, "just in case"
	
	startTime: DateTime,
	endTime: DateTime,
	days: Days,
	
	nTimes: Long,
	nTwits: Long,
	nInits: Long,	
	nReplies: Long
)

case class WordRole(name: String) {
  val words = WordPeople(name+" words")
  val users = WordPeople(name+" users")
  val tags  = WordPeople(name+" tags")
  val urls  = WordPeople(name+" urls")
 
  override def toString = name+":WordRole:\n"+List(words,users,tags,urls).foldLeft("")(_+"\n"+_)
}

case class WordInitiators() extends WordRole("initiators")
case class WordRepliers()   extends WordRole("repliers")
case class WordMumblers()   extends WordRole("mumblers")
case class WordTweeters()   extends WordRole("tweeters")

abstract class TVisitorWordsBase(
	val lowerCase: Boolean,
	val twitProgress: Option[Long],
	val progress: WordUserProgress) extends TVisitor {
	
  var everySoOften: List[ActOften] = List()
	
  def addTwitRole(wr: WordRole, t: Twit) = {
    val tzer = new TwitTokens(t.text) 
    for (token <- tzer) {
		token match {
			case TwitTokenUser(w) => wr.users.addUser(w, t.uid, progress)
			case TwitTokenTag(w)  => wr.tags.addUser(w, t.uid,  progress)
			case TwitTokenUrl(w)  => wr.urls.addUser(w, t.uid,  progress)
			case TwitTokenWord(w) => wr.words.addUser(w, t.uid, progress)
			case TwitTokenNone() => // TODO shouldn't happen?
		}
    }
  }
  
  // doTwit still undefined, see derived!
  def toString: String
}


class TVisitorWordsByRole(
	override val lowerCase: Boolean,
	override val twitProgress: Option[Long],
	override val progress: WordUserProgress) extends TVisitorWordsBase(
		lowerCase, twitProgress, progress) {

  val initiators = WordInitiators()
  val repliers   = WordRepliers()
  val mumblers   = WordMumblers()
  
  def doTwit(t: Twit) = {
  	t.role match {
  		case TwitInit(_)    => addTwitRole(initiators, t)
  		case TwitReply(_,_) => addTwitRole(repliers, t)
  		case TwitMumble()   => addTwitRole(mumblers, t)
  	}
  }
  
  // def joinRoles: WordTweeters
  
  override def toString = "words by role:\n"+List(initiators,repliers,mumblers).foldLeft("")(_+"\n"+_)
}


class TVisitorWordsTogether(
	override val lowerCase: Boolean,
	override val twitProgress: Option[Long],
	override val progress: WordUserProgress) extends TVisitorWordsBase(
		lowerCase, twitProgress, progress) {

	val tweeters = WordTweeters()

	// could make addTwitRole curried
	def doTwit(t: Twit) = addTwitRole(tweeters, t)
	
	override def toString = "words together: "+tweeters
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

    maxTwits:  Option[Long],
    byRole:    Option[Boolean],
    lowerCase: Option[Boolean],
    
    twitProgress: Option[Long],
    wordProgress: Option[Long],
    userProgress: Option[Long],
    pairProgress: Option[Long],
    args: Array[String]) = {
    
    val lowerCase_   = lowerCase getOrElse false
    val wordUserProgress = WordUserProgress(wordProgress, userProgress, pairProgress)

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
          
    val tv = byRole match {
    	case Some(b) if b => new TVisitorWordsByRole(
									  lowerCase_,
									  twitProgress,
									  wordUserProgress)
		case _            => new TVisitorWordsTogether(
									  lowerCase_,
									  twitProgress,
									  wordUserProgress)
		}		
    
    maxTwits match {
      case Some(atMost) =>
        err.println("doing at most "+atMost+" tweets")
        twitCorpus.visitTake(atMost)(tv)
      case _ => 
        err.println("doing ALL tweets -- may take a LONG time!")
        twitCorpus.visitAll(tv)
    }
    
    err.println("the final stats of word/user-sets")
    err.println(tv)
  }
}
