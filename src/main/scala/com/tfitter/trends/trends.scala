package com.tfitter.trends

import java.util.BitSet
import java.io.{FileOutputStream,BufferedOutputStream}

import org.joda.time.{DateTime,Days}
import org.scala_tools.time.Imports._
import scala.collection.mutable.{Map,Set}

import org.suffix.util.bdb.{BdbFlags, BdbArgs}

import com.tfitter.db.types.UserID // {UserID => User}
import com.tfitter.db.{Twit,TwitRole,TwitInit,TwitReply,TwitMumble}

import com.tfitter.tokenizer._
import com.tfitter.corpus.{TVisitor,TwitterCorpus,CountOften,ActOften}
import com.tfitter.corpus.RichTokenizedLM._
import System.err


object types {
    type Word     = String
    type Users    = Set[UserID]
    type UserDays = Map[UserID,DaySet]
    // TODO use SortedMap or jcl.TreeMap:
    type DaySizes = Map[Int,Int]
    type WordTabs = Map[Word,WordInfo]
}
import types._


// day of year from 0 to Total_days in study
object DaySet {
    var epoch = new DateTime("2009-06-15")
    val startBits = 16 // default starting size for java.util.BitSet
    def apply(startBits: Int): DaySet = new DaySet(startBits)
    // NB when declared as apply: DaySet,
    // couldn't invoke factory as = DaySet()!
    // http://paste.pocoo.org/show/145944/
    def apply(): DaySet = new DaySet(startBits)
}
class DaySet(val startBits: Int) {
    val bits = new java.util.BitSet(startBits)
    
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
    
    // the number of the set bits, i.e. of the elements in the set
    def cardinality: Int = bits.cardinality
}


case class WordUserProgress(
    word: Option[Long],
    user: Option[Long],
    pair: Option[Long],
    prune: Boolean
)


object WordInfo {
    // TODO may be needed for dayPeopleSize array length
    var totalDays = 50
}


case class WordInfo (   
    var start: DateTime
) {
    var end:   DateTime = start
        
    var nTimes:   Long = 0
    var nTwits:   Long = 0
    var nInits:   Long = 0
    var nReplies: Long = 0

    val days = DaySet()
    val userDays: UserDays = Map()

    // sizes of userDays per day
    // TODO we might want to keep it around once computed, or not
    // lazy var dayPeopleSizes: DaySizes = countDayPeopleSizes

    def countDayPeopleSizes: DaySizes = { 
    	val dayPeopleSizes: DaySizes = Map()
    	userDays foreach { case (_, dayset) =>
    		dayset.indices foreach { case day =>
				dayPeopleSizes.get(day) match {
					case Some(_) => dayPeopleSizes(day) += 1
					case _ => dayPeopleSizes(day) = 1
				}
			}
    	}
    	dayPeopleSizes
    }
   
    
    def writeDayPeopleSizes(dayPeopleSizes: DaySizes, bout: BufferedOutputStream) = {
    
    	// TODO keep dayPeopleSizes in a SortedMap to begin with,
    	// then this sorting is unnecessary:
    	// List sorting is slow, don't do that
		// (dayPeopleSizes.toList sortWith (_._1 < _._1))
		
		val sortedDays = dayPeopleSizes.keySet.toArray // not yet:
		util.Sorting.quickSort(sortedDays)
		
		sortedDays foreach { case day =>
			bout.write(("\t"+day+" "+dayPeopleSizes(day)).getBytes)
		}
  	}
  	
  	
  	def countWriteDayPeopleSizes(bout: BufferedOutputStream) = 
  		writeDayPeopleSizes(countDayPeopleSizes, bout)	
    
}


class Histogram[T <% Ordered[T]] {
	val h: Map[T,Int] = Map()
	
	def inc(t: T): Unit = {
		h.get(t) match {
			case Some(_) => h(t) += 1
			case _ => h(t) = 1
		}
	}
	
	def toList: List[(T,Int)] = {
		h.toList sort { case (a,b) => a._2 > b._2 || (a._2 == b._2 && a._1 > b._1) }
	}
	
	override def toString: String = {
		toList.mkString
	}
}


// NB could make WordUserProgress constructor argument
case class WordPeople(name: String) {
  val words: WordTabs = Map.empty

  // cache results of users() call
  var users: Users = Set.empty
  
  var nWords: Long = 0
  var nUsers: Long = 0
  var nWordUsers: Long = 0
  
  def size: Long = words.size // == nWords

  // this is called when w
  def addWordUser(info: WordInfo, user: UserID, progress: WordUserProgress) = {
  	// TODO why can't we say DaySet() here?:
    info.userDays(user) = DaySet()
    nWordUsers += 1
    progress.pair match {
        case Some(n) if nWordUsers % n == 0 => print(":")
        case _ =>
    }
    if (!users.contains(user)) {
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
  
  def sizeHistogram: Histogram[Int] = {
  	val h = new Histogram[Int]()
  	words foreach { case (word,info) =>
  		val size = info.userDays.size
  		h.inc(size)
  	}
  	h
  }
  
  def writeDayPeopleSizes(fileNamePrefix: String, progress: Option[Long]): Unit = {
  	val fileName = fileNamePrefix+"-"+name
  	val bout = new BufferedOutputStream(new FileOutputStream(fileName))
  	err.print("writing word role "+name+" to: "+fileName+"... ")
  
  	// TODO other ways to store WordTabs sorted or sort Map?
  	val sortedWords = words.keySet.toArray
  	util.Sorting.quickSort(sortedWords)
  	
  	var wordCount = 0
  	sortedWords foreach { key =>
  		bout.write(key.getBytes)
  		words(key).countWriteDayPeopleSizes(bout)
  		wordCount += 1
  		progress match {
  			case Some(n) if wordCount % n == 0 => err.print(".")
  			case _ =>
  		}
  		bout.write("\n".getBytes)
  	}
  	bout.close
  	err.println("done")
  }
  
  def resetUsers: Users = {
    // TODO may explicitly parameterize with case (k,v) for clarity:
    users = words.foldLeft(Set.empty: Users)(_++_._2.userDays.keySet)
    users
  }
  
  def resetWordsSize: Long = {
    nWords = words.size
    nWords
  }
  
  def resetUsersSize: Long = {
    resetUsers
    nUsers = users.size
    nUsers
  }
  
  // NB can compute both nUsers and nWordUsers in one fell fold
  def resetWordUsersSize: Long = {
    // TODO may explicitly parameterize with case (k,v) for clarity:
    nWordUsers = words.foldLeft(0)(_+_._2.userDays.foldLeft(0)(_+_._2.cardinality))
    nWordUsers
  }
  
  override def toString =
    name+":WordPeople: has "+nWords+" words, "+nUsers+" people, "+nWordUsers+" total pairs"

  
  def prune(minCount: Int, progress: Boolean)(twitCount: Long): Unit = {
  	var prunedCount = 0
  	val wordsSize = words.size
  	val (nWords_,nUsers_,nWordUsers_) = (nWords,nUsers,nWordUsers)
  	words foreach { case (word,info) =>
  		if (info.userDays.size < minCount) { 
  			words.removeKey(word) // TODO remove in 2.8
  			prunedCount += 1
  		}
  	}
  	resetWordsSize
    resetUsersSize
    resetWordUsersSize
  	if (progress) err.println(name+" "+prunedCount+" words pruned at "+
  		twitCount+" twits, bringing words size from "+wordsSize+" to "+words.size+
  		"\n before: nWords="+nWords_ +" nUsers="+nUsers_ +" nWordUsers="+nWordUsers_ +
  		"\n after:  nWords="+nWords  +" nUsers="+nUsers  +" nWordUsers="+nWordUsers)
  }
}


case class WordRole(name: String) {
  val words = WordPeople(name+"-words")
  val users = WordPeople(name+"-users")
  val tags  = WordPeople(name+"-tags")
  val urls  = WordPeople(name+"-urls")
  val all = List(words,users,tags,urls)
  
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
 
  override def toString = name+" : WordRole"+all.foldLeft("")(_+"\n"+_)
  
  def writeWordDayPeopleSizes(filePrefix: String, progress: Option[Long]) = {
  	all foreach { 
  		_.writeDayPeopleSizes(filePrefix, progress) 
  	}
  }
  
  def wordSizeHistogram: String = {
  	name+" word size histogram: "+
  	// NB was interspersing one character per line:
  	// (all map { _.sizeHistogram }) mkString "\n"
  	(all.map { case role => role.name+" "+role.sizeHistogram }).mkString("\n")
  }
  
  def prune(minCount: Int, progress: Boolean)(twitCount: Long) = {
  	all foreach { words =>
  		words.prune(minCount, progress)(twitCount)
  		// err.println("DEBUG pruned "+words.name+" down to size:"+words.size)
  	}
  }
}

case class WordInitiators() extends WordRole("initiators")
case class WordRepliers()   extends WordRole("repliers")
case class WordMumblers()   extends WordRole("mumblers")
case class WordTweeters()   extends WordRole("tweeters")

abstract class TVisitorWordsBase(
    val lowerCase: Boolean,
    val twitProgress: Option[Long],
    val progress: WordUserProgress,
    val pruneCountOften: Option[CountOften]) extends TVisitor {
    
  var twitCount: Long = 0
  var everySoOften: List[ActOften] = List()
    
  // TODO could keep our own twitCount incremented,
  // or just access the original TwitCorpus's one
  // and leave doTwit(t: Twit) without count
  
  def doTwit(t: Twit, tc: Long) = {
  	twitCount = tc
  }
  
  def toString: String
  
  def writeWordDayPeopleSizes(
  	filePrefix: String,
  	progress: Option[Long]): Unit
  	
  def writeWordSizeHistogram(
  	fileName: String)

  def prune(minCount: Int, progress: Boolean)(twitCount: Long)
  
  everySoOften = pruneCountOften match {
  	case Some(CountOften(often,count)) => 
  		List(ActOften(often,
  			prune(count,progress.prune)))
  	case _ => List()
  	}

}


class TVisitorWordsByRole(
    override val lowerCase: Boolean,
    override val twitProgress: Option[Long],
    override val progress: WordUserProgress,
    override val pruneCountOften: Option[CountOften]) extends TVisitorWordsBase(
        lowerCase, twitProgress, progress, pruneCountOften) {

  val initiators = WordInitiators()
  val repliers   = WordRepliers()
  val mumblers   = WordMumblers()
  val all = List(initiators,repliers,mumblers)
  
  override def doTwit(t: Twit, tc: Long) = {
  	super.doTwit(t, tc)
    t.role match {
        case TwitInit(_)    => initiators.addTwit(t, progress)
        case TwitReply(_,_) => repliers.addTwit(t, progress)
        case TwitMumble()   => mumblers.addTwit(t, progress)
    }
  }
  
  // def joinRoles: WordTweeters
  
  override def toString = "words by role:"+all.foldLeft("")(_+"\n"+_)+"\n"

  def writeWordDayPeopleSizes(filePrefix: String, progress: Option[Long]) = {
  	all foreach { 
  		_.writeWordDayPeopleSizes(filePrefix, progress)
  	}
  }

  def writeWordSizeHistogram(fileName: String) = {
  	val bout = new BufferedOutputStream(new FileOutputStream(fileName))
  	val hist = (all map { _.wordSizeHistogram }).mkString("")
  	bout.write(hist.getBytes)
	bout.close
  }
  
  def prune(minCount: Int, progress: Boolean)(twitCount: Long) = {
  	all foreach { role =>
  		role.prune(minCount,progress)(twitCount)
  	}
  }
}


class TVisitorWordsTogether(
    override val lowerCase: Boolean,
    override val twitProgress: Option[Long],
    override val progress: WordUserProgress,
    override val pruneCountOften: Option[CountOften]) extends TVisitorWordsBase(
        lowerCase, twitProgress, progress, pruneCountOften) {

    val tweeters = WordTweeters()

    // could make addTwitRole curried
    override def doTwit(t: Twit, tc: Long) = {
    	super.doTwit(t, tc)
    	tweeters.addTwit(t, progress)
    }
    
    override def toString = "words together: "+tweeters
    
    def writeWordDayPeopleSizes(filePrefix: String, progress: Option[Long]) = {
  		tweeters.writeWordDayPeopleSizes(filePrefix, progress)
  	}

	def writeWordSizeHistogram(fileName: String) = {
		val bout = new BufferedOutputStream(new FileOutputStream(fileName))
		val hist = tweeters.wordSizeHistogram
		bout.write(hist.getBytes)
		bout.close
	}

	def prune(minCount: Int, progress: Boolean)(twitCount: Long) = {
		tweeters.prune(minCount,progress)(twitCount)
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

    maxTwits:  Option[Long],
    byRole:    Option[Boolean],
    lowerCase: Option[Boolean],
    
    // TODO
    // prune: Option[String]
    // then get Either a pair or one (final) pruning mincount
    
	pruneCount: Option[Int],
	// pruneOften: Option[Long],
	// TODO pruneOften is not respected on Rocket!
	pruneEvery: Option[Long],
	showPrune:  Option[Boolean],
	
    poolProgress: Option[Long],
    twitProgress: Option[Long],    
    wordProgress: Option[Long],
    userProgress: Option[Long],
    pairProgress: Option[Long],
    
    histFile: Option[String],
    dumpProgress: Option[Long], // words histogram

    wordFile: Option[String],  
    args: Array[String]) = {
      
    val pruneOften = pruneEvery
    
    def givenOrPooled(given: Option[Long], pool: Option[Long]): Option[Long] = 
    	if (given.isEmpty) pool
    	else given

	// TODO add to my util:
	val showPrune_ = showPrune match {
		case Some(bool) if (bool) => true
		case _ => false
		}
		
	val twitProgress_ = givenOrPooled(twitProgress, poolProgress)    

    val wordUserProgress = WordUserProgress(
    	givenOrPooled(wordProgress, poolProgress),
    	givenOrPooled(userProgress, poolProgress),
    	givenOrPooled(pairProgress, poolProgress),
    	showPrune_)
    	
	val dumpProgress_ = givenOrPooled(dumpProgress, poolProgress)    
    

    val lowerCase_   = lowerCase getOrElse false
    
    // val pruneOften=Some(50000)
    err.println("pruneOften="+pruneOften)
    val pruneCountOftenOpt = pruneOften match {
    	case Some(often) =>
    		err.println(" pruneCountOftenOpt is Some!")
    		Some(CountOften(often,
    			pruneCount match {
    				case Some(count) => count
    				case _ => 2 // TODO DEFAULT
    				}))
    	case _ => None
    }
    	

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
                                      twitProgress_,
                                      wordUserProgress,
                                      pruneCountOftenOpt)
        case _            => new TVisitorWordsTogether(
                                      lowerCase_,
                                      twitProgress_,
                                      wordUserProgress,
                                      pruneCountOftenOpt)
        }    
        
    err.println("the twit visitor has "+tv.everySoOften.size+" periodic actions:"+tv.everySoOften)
    
    maxTwits match {
      case Some(atMost) =>
        err.println("doing at most "+atMost+" tweets")
        twitCorpus.visitTake(atMost)(tv)
      case _ => 
        err.println("doing ALL tweets -- may take a LONG time!")
        twitCorpus.visitAll(tv)
    }
    
    val gonnaPrune = pruneCount match {
    	case Some(n) =>  " before pruning to under "+n+" count:"
    	case _ => ""
    	}
    	
    err.println("\nthe final stats of word/user-sets"+gonnaPrune)
    err.println(tv)
    
    pruneCount match { 
    	case Some(minCount) if (pruneOften.isEmpty) =>
    		tv.prune(minCount,showPrune_)(tv.twitCount)
			err.println("the final stats of word/user-sets after pruning:")
			err.println(tv)
		case _ =>
    }
    
    wordFile match {
		case Some(prefix) => tv.writeWordDayPeopleSizes(prefix, dumpProgress_)
		case _ =>
    }
    
    histFile match {
    	case Some(file) => tv.writeWordSizeHistogram(file)
    	case _ =>
    }
  }
}

// typical command line invocation:

