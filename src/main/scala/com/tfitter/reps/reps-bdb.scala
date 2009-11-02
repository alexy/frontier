package com.tfitter.db.graph

import System.err

import com.tfitter.db.types._
import com.tfitter.Repliers
import com.tfitter.Serialized.loadRepliers

import org.suffix.util.bdb.{BdbArgs,BdbFlags,BdbStore}

import com.sleepycat.persist.model._
import com.sleepycat.persist.model.Relationship.MANY_TO_ONE

import scala.collection.mutable.{Map=>UMap}

import java.lang.{Integer=>JInt,Long=>JLong}
import java.util.{HashMap=>JHMap}

object types {
  type RePair = (UserID,(TwitCount,TwitCount))
  type RepCount = UMap[UserID,(TwitCount,TwitCount)]
  type ReplierMap = UMap[Int,RepCount]
  type FixedRepliers = Map[UserID, Map[UserID,(TwitCount,TwitCount)]]
}
import types._


case class RepPair (
  s: UserID,
  t: UserID,
  reps: TwitCount,
  dirs: TwitCount
  )
  
case class UserReps (
  s: UserID,
  rc: RepCount
  )

@Persistent
class EdgeBDB {
  @KeyField(1) var s: JInt = null
  @KeyField(2) var t: JInt = null
  def this(_s: Int, _t: Int) = { this()
    s = _s
    t = _t
  }
}
   
@Entity
class RepPairBDB {
  // or can just shift two Ints into a Long
  @PrimaryKey
  var st: EdgeBDB = new EdgeBDB
  var reps: JInt = null
  var dirs: JInt = null
  def this(_s: UserID, _t: UserID, _reps: TwitCount, _dirs: TwitCount) = { this()
    st = new EdgeBDB(_s, _t)
    reps = _reps
    dirs = _dirs
  }
  def this(rp: RepPair) = { this()
    st = new EdgeBDB(rp.s,rp.t)
    reps = rp.reps
    dirs = rp.dirs
  }
  def toRepPair: RepPair = RepPair(st.s.intValue,st.t.intValue,reps.intValue,dirs.intValue)
}

@Entity
class UserRepsBDB {
  @PrimaryKey
  var s: java.lang.Integer = null
  var t: JHMap[JInt,JInt] = null
  var u: JHMap[JInt,JInt] = null
  def this(_s: UserID, rc: RepCount) = { this()
  // Map(1->(2,3),4->(5,2),2->(7,8)).foldLeft(Nil:List[(Int,Int)],Nil:List[(Int,Int)]) { 
  // case ((acc1, acc2), (x, (y, z))) => ( (x,y)::acc1, (x,z)::acc2) }  
    s = _s
    t = new JHMap
    u = new JHMap
    rc foreach { case (x, (y, z)) => t.put(x,y); u.put(x,z) }    
  }
  def toUserReps: UserReps = {
    val sid = s.intValue
    var m: RepCount = UMap.empty
    val tit = t.entrySet.iterator
    val uit = u.entrySet.iterator
    while (tit.hasNext) {
      val tp = tit.next
      val up = uit.next
      val s = tp.getKey
      assert (up.getKey == s)
      val t = tp.getValue
      val u = up.getValue
      // m += (s -> (t,u))
      m(s.intValue) = (t.intValue,u.intValue)
    }
    UserReps(sid,m)
  }
}


trait RepsBDB {
  def saveMap(r: Repliers, showProgress: Boolean): Unit
  def loadMap(showProgress: Boolean): ReplierMap
  def getReps(u: UserID): Option[RepCount]
  def repMapCacheStats: String
}


class RepliersBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) with RepsBDB {
  val rpPrimaryIndex =
    store.getPrimaryIndex(classOf[EdgeBDB], classOf[RepPairBDB])
      
  def saveMap(r: Repliers, showProgress: Boolean): Unit = {
    var edgeCount = 0
    for ((s,st) <- r.reps; (t,(reps,dirs)) <- st) {
      val edge = new RepPairBDB(s,t,reps,dirs)
      rpPrimaryIndex.put(txn, edge)
      edgeCount += 1
      if (showProgress && edgeCount % 100000 == 0) err.print('.')
    }
    err.println
  }
  
  def loadMap(showProgress: Boolean): ReplierMap = { 
    val curIter = new CursorIterator(rpPrimaryIndex.entities)
    var reps: ReplierMap = UMap()

    var edgeCount = 0
    for (ej <- curIter) {
      val e: RepPair = ej.toRepPair
      if (reps.contains(e.s)) reps(e.s)(e.t) = (e.reps,e.dirs)
      // = Map(x->y) is harder to do with UMap,
      // hence FixedRepliers -- and immutable at that!
      // alas, we have to cast back to mutable ReplierMap
      // to fit the trait RepsBDB which also fits RepMaps
      else reps(e.s) = UMap(e.t -> (e.reps,e.dirs))
      edgeCount += 1
      if (showProgress && edgeCount % 100000 == 0) err.print('.')
    }
    err.println
    reps
  }
  
  def getReps(u: UserID): Option[RepCount] = {
    val curIter = new CursorIterator(rpPrimaryIndex.entities(
      new EdgeBDB(u, 0), true,
      new EdgeBDB(u, Math.MAX_INT), true))
    val rc: RepCount = UMap.empty
    
    for (ej <- curIter) {
      val e: RepPair = ej.toRepPair
      assert(e.s == u)
      rc(e.t) = (e.reps,e.dirs)
    }
    // == UMap.empty or count edges inserted vs 0:
    if (rc == UMap.empty) None
    else Some(rc)
  }

  def repMapCacheStats: String = "cache not implemented for triples yet"
}


object StoreRepliersBDB extends optional.Application {
  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    showProgress: Option[Boolean],
    args: Array[String]) = {
      
    val repSerName: String = args(0) // need it

    val bdbEnvPath   = envName getOrElse "reps.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repliers"// Config.bdbStoreName
    val bdbCacheSize = cacheSize match {
      case Some(x) => Some((x*1024*1024*1024).toLong)
      case _ => None // Config.bdbCacheSize
    }
    val bdbFlags = BdbFlags(
      allowCreate   getOrElse false,
      readOnly      getOrElse false,
      transactional getOrElse false,
      deferredWrite getOrElse false,
      noSync        getOrElse false
    )
    val bdbArgs = BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)

    // make this a parameter:
    val showingProgress = showProgress getOrElse true

    val repsDb = new RepliersBDB(bdbArgs)
    
    val reps: Repliers = loadRepliers(repSerName)
    
    err.print("Saving Repliers to Berkeley DB... ")
    repsDb.saveMap(reps,showingProgress)
    err.println("done")
  }
}


object FetchRepliersBDB extends optional.Application {
  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    showProgress: Option[Boolean],
    args: Array[String]) = {
      

    val bdbEnvPath   = envName getOrElse "reps.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repliers"// Config.bdbStoreName
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

    // make this a parameter:
    val showingProgress = showProgress getOrElse true

    val repsDb = new RepliersBDB(bdbArgs)
    
    err.print("Loading Repliers from Berkeley DB... ")
    val reps: ReplierMap = repsDb.loadMap(showingProgress)
    err.println("done")
  }
}


class RepMapsBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) with RepsBDB {
  val urPrimaryIndex =
    store.getPrimaryIndex(classOf[JInt], classOf[UserRepsBDB])
      
  def saveMap(r: Repliers, showProgress: Boolean): Unit = {
    var userCount = 0
    for ((s,rc) <- r.reps) {
      val userReps = new UserRepsBDB(s,rc)
      urPrimaryIndex.put(txn, userReps)
      userCount += 1
      if (showProgress && userCount % 100000 == 0) err.print('.')
    }
    err.println
  }
  
  def loadMap(showProgress: Boolean): ReplierMap = { 
    val curIter = new CursorIterator(urPrimaryIndex.entities)
    var reps: ReplierMap = UMap.empty

    var userCount = 0
    for (uj <- curIter) {
      val ur: UserReps = uj.toUserReps
      reps(ur.s) = ur.rc
      userCount += 1
      if (showProgress && userCount % 100000 == 0) err.print('.')
    }
    err.println
    reps
  }

  var repMapCache: ReplierMap = UMap.empty
  var repMapCacheHits: Long   = 0
  var repMapCacheMisses: Long = 0
  
  def getReps(u: UserID): Option[RepCount] = {
    if (repMapCache contains u) {
      repMapCacheHits += 1
      Some(repMapCache(u))
    }
    else {
      repMapCacheMisses += 1      
      val uj = urPrimaryIndex.get(u)
      uj match {
        case null => None
        case ur => val reps = ur.toUserReps.rc
          repMapCache(u) = reps
          Some(reps)
      }
    }
  }

  def repMapCacheStats: String =
    "replier map hits: "+repMapCacheHits+", misses: "+repMapCacheMisses+
            ", total: "+(repMapCacheHits+repMapCacheMisses)
}


object StoreUserRepsBDB extends optional.Application {
  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    showProgress: Option[Boolean],
    args: Array[String]) = {
      
    val repSerName: String = args(0) // need it

    val bdbEnvPath   = envName getOrElse "urs.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repmaps"// Config.bdbStoreName
    val bdbCacheSize = cacheSize match {
      case Some(x) => Some((x*1024*1024*1024).toLong)
      case _ => None // Config.bdbCacheSize
    }
    val bdbFlags = BdbFlags(
      allowCreate   getOrElse false,
      readOnly      getOrElse false,
      transactional getOrElse false,
      deferredWrite getOrElse false,
      noSync        getOrElse false
    )
    val bdbArgs = BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)

    // make this a parameter:
    val showingProgress = showProgress getOrElse true

    val ursDb = new RepMapsBDB(bdbArgs)
    
    val reps: Repliers = loadRepliers(repSerName)
    
    err.print("Saving Repliers to Berkeley DB... ")
    ursDb.saveMap(reps,showingProgress)
    err.println("done")
  }
}


object FetchUserRepsBDB extends optional.Application {
  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    showProgress: Option[Boolean],
    args: Array[String]) = {
      

    val bdbEnvPath   = envName getOrElse "urs.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repmaps"// Config.bdbStoreName
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

    // make this a parameter:
    val showingProgress = showProgress getOrElse true

    val ursDb = new RepMapsBDB(bdbArgs)
    
    err.print("Loading Repliers from Berkeley DB... ")
    val reps: ReplierMap = ursDb.loadMap(showingProgress)
    err.println("done")
  }
}
