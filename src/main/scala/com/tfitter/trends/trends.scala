package com.tfitter.trends

import java.util.BitSet
import org.joda.time.{DateTime,Days}
import org.scala_tools.time.Imports._
import scala.collection.mutable.{Map,Set}

import org.suffix.util.bdb.{BdbFlags, BdbArgs}

import com.tfitter.db.types.UserID // {UserID => User}
import com.tfitter.db.{Twit,TwitRole,TwitInit,TwitReply,TwitMumble}

import com.tfitter.tokenizer._
import com.tfitter.corpus.{TVisitor,TwitterCorpus,ActOften}
import com.tfitter.corpus.RichTokenizedLM._
import System.err


// day of year from 0 to Total_days in study
object DaySet {
    var epoch = new DateTime("2008-01-01")
}
class DaySet {
    val bits = new java.util.BitSet
    
    def set(dt: DateTime) = {
        val eDay = Days.daysBetween(DaySet.epoch,dt).getDays
        bits.set(eDay)
    }
    
    def indices/*(bits: java.util.BitSet)*/: List[Int] = {
        def aux(i: Int, li: List[Int]): List[Int] = {
            val next = bits.nextSetBit(i)
            if (next < 0) li.reverse
            else aux(next+1,next::li)
        }
        aux(0,List())
    }
}

case class WordUserProgress(
    word: Option[Long],
    user: Option[Long],
    pair: Option[Long]
)


object WordInfo {
    // TODO may be needed for dayPeopleSize array length
    var totalDays = 50
}



case class WordInfo (   
    var start: DateTime
) {
    var end:   DateTime = start

    val days = new DaySet
    val userDays: types.UserDays = Map()
    
    // sizes of userDays per day
    var dayPeopleSize: Array[Int] = Array()
    
    var nTimes:   Long = 0
    var nTwits:   Long = 0
    var nInits:   Long = 0
    var nReplies: Long = 0
}


object types {
    type Word = String
    type Users = Set[UserID]
    type UserDays = Map[UserID,DaySet]
    type WordTabs = Map[Word,WordInfo]
}
import types._


// NB could make WordUserProgress constructor argument
case class WordPeople(name: String) {
  val words: WordTabs = Map.empty
  val users: Users = Set.empty
  var nWords = 0 // == words.size
  var nUsers = 0 // == users.size
  var nWordUsers = 0

  // this is called when w
  def addWordUser(info: WordInfo, user: UserID, progress: WordUserProgress) = {
    info.userDays(user) = new DaySet
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

  def addToken(token: Word, t: Twit, newForTwit: Boolean, progress: WordUserProgress): Unit = {
        val info = 
        (words get token) match {
            case Some(info) => 
                if (!info.userDays.contains(t.uid)) { 
                    addWordUser(info, t.uid, progress)
                }
                info
            case _ => 
                val info =  WordInfo(t.time)
                words(token) = info
                nWords += 1
                progress.word match {
                    case Some(n) if nWords % n == 0 => print("x")
                    case _ =>
                }
                addWordUser(info, t.uid, progress)
                info
        }

        if (info.start > t.time) info.start = t.time
        if (info.end < t.time)   info.end = t.time
        
        info.days.set(t.time)
        info.userDays(t.uid).set(t.time)
        
        info.nTimes += 1
        if (newForTwit) info.nTwits  += 1
        if (t.isReply) info.nReplies += 1 // may be several per reply!
        if (t.isInit)  info.nInits   += 1 // may be several per reply!      
  }
  
  override def toString =
    name+":WordPeople: has "+nWords+" words, "+nUsers+" people, "+nWordUsers+" total pairs"
}


case class WordRole(name: String) {
  val words = WordPeople(name+" words")
  val users = WordPeople(name+" users")
  val tags  = WordPeople(name+" tags")
  val urls  = WordPeople(name+" urls")
  
  
  def addTwit(t: Twit, progress: WordUserProgress) = {
    val tzer = new TwitTokens(t.text)
    val tokensSeen: Set[TwitToken] = Set.empty
    for (token <- tzer) {
        val newForTwit = !tokensSeen.contains(token)
        token match {
            case TwitTokenUser(w) => users.addToken(w, t, newForTwit, progress)
            case TwitTokenTag(w)  => tags.addToken(w, t,  newForTwit, progress)
            case TwitTokenUrl(w)  => urls.addToken(w, t,  newForTwit, progress)
            case TwitTokenWord(w) => words.addToken(w, t, newForTwit, progress)
            case TwitTokenNone() => // TODO shouldn't happen?
        }
        tokensSeen += token
    }
  }
 
  override def toString = name+" : WordRole"+List(words,users,tags,urls).foldLeft("")(_+"\n"+_)
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
        case TwitInit(_)    => initiators.addTwit(t, progress)
        case TwitReply(_,_) => repliers.addTwit(t, progress)
        case TwitMumble()   => mumblers.addTwit(t, progress)
    }
  }
  
  // def joinRoles: WordTweeters
  
  override def toString = "words by role:"+List(initiators,repliers,mumblers).foldLeft("")(_+"\n"+_)+"\n"
}


class TVisitorWordsTogether(
    override val lowerCase: Boolean,
    override val twitProgress: Option[Long],
    override val progress: WordUserProgress) extends TVisitorWordsBase(
        lowerCase, twitProgress, progress) {

    val tweeters = WordTweeters()

    // could make addTwitRole curried
    def doTwit(t: Twit) = tweeters.addTwit(t, progress)
    
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
