package com.tfitter.db.graph

import System.err

import types._
import com.tfitter.db.types._

import scala.util.Sorting.stableSort
import org.suffix.util.bdb.{BdbArgs,BdbFlags,BdbStore}
import org.suffix.util.input.Ints


object PairsBDB extends optional.Application {
 
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
    usePairs: Option[Boolean],
    csv: Option[Boolean],
    showFringe: Option[Boolean],
    pair: Option[String],
    args: Array[String]) = {
      
    // csv lists community members only, without edges/ties
    val csv_ = csv getOrElse false
    val showFringe_ = showFringe getOrElse false
    val usingPairs = usePairs getOrElse false
    
    val bdbEnvPath   = envName getOrElse (if (usingPairs) "reps.bdb" else "urs.bdb")
    val bdbStoreName = storeName getOrElse (if (usingPairs) "repliers" else "repmaps")
    
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

    val udb = if (usingPairs) new RepliersBDB(bdbArgs)
      else new RepMapsBDB(bdbArgs)
      
    err.println("udb class: "+udb.getClass)

    // val u1: UserID = 29524566 // Just_toddy   
    // val u2: UserID = 12921202 // myleswillsaveus
    // println("repliers for "+u1+udb.getReps(u1))
    // println("repliers for "+u2+udb.getReps(u2))
    
    import Communities._
    val c = new Communities(udb.getReps _)
    // the pairs in a different order yield
    // a different result set!
    
    val repPairs = 
    if (!pair.isEmpty) {
      err.println("# for the pair: "+pair.get)
      List(Ints.getPair(pair.get)) // pardon the palindrome! 
    } else if (args.length < 1) {
      val repCooked = List[UserPair](
      (25688017,14742479),(14742479,25688017),
      (30271870,26073346),(26073346,30271870),
      (12921202,29524566),(29524566,12921202))
      err.println("# using pre-cooked repPairs list:"+repCooked)
      repCooked
    } else {
      val repPairsFile = args(0)
      err.println("# reading repPairs from "+repPairsFile)
      Ints.readPairs(repPairsFile) 
    }
        
    repPairs.foreach { up: UserPair =>
      val com: Community = c.triangles(up,Some(100),None)
      println("# "+up+" community:")
      println(c.showCommunity(com,csv_))
      if (showFringe_) {
        println("# "+up+" fringe:")
        println(c.fringeUsers(com) mkString ",")
      }
      println("# cache stats: "+udb.repMapCacheStats)
      println("#"+"-"*50)
    }
  }
} 

object Communities {
  import scala.collection.immutable.Queue
  type Gen = Int
  type Tie = Int
  type UserSet  = Set[UserID]
  type UserList = List[UserID]
  type UserPair = (UserID,UserID)
  type TiesPair = (Tie,Tie)
  type UserPairQueue = Queue[(UserPair,Gen)]
  type UserTies = (UserID,TiesPair)
  type ComMember = (UserID,UserPair,TiesPair,Gen)
  type RepPairs  = List[UserPair]
  type Community = List[ComMember]
  type ComTroika = (UserPairQueue,UserSet,Community)
  type FringeUser = (UserPair, TiesPair)
  type Fringe = Set[FringeUser]
  
}

class Communities(getReps: UserID => Option[RepCount]) {
  import scala.collection.immutable.Queue
  import Communities._

  def triangles(up: UserPair,
    maxTotal: Option[Int], maxGen: Option[Int]): Community = {
    val (u1,u2) = up  
      
    def firstGreater(a: RePair,b: RePair) = a._1 > b._1
  
    def common(a: List[RePair], b: Array[RePair], haveSet: UserSet): Option[UserTies] = {
      def aux(l: List[RePair]): Option[UserTies] = l match {
        case (s,(t1,u))::xs => if (haveSet contains s) aux(xs)
        // We rely on Array a.find finding leftmost item here, 
        // so it is the highest rank -- must confirm from Scala source!
        else b.find(_._1==s) match {
          case Some((_,(t2,_))) => Some((s,(t1,t2)))
          case _ => aux(xs)
        }
        case _ => None
      }
      aux(a)
    }
    
    def addPair(troika: ComTroika): (ComTroika,Boolean) = {
      val (pairs,haveSet,community) = troika
      if (pairs.isEmpty) return (troika,false)
      
      val ((parents @ (u1,u2),parentGen),deQueue) = pairs.dequeue
      // println("dequeued "+parents)
      
      // can we unify return deQueue...true into a single point or single value?
      
      if (!maxGen.isEmpty && parentGen >= maxGen.get)   return ((deQueue,haveSet,community),true)
      // need { return ... } in a block, or illegal start of a simple expression:
      val u1reps: RepCount = getReps(u1) getOrElse    { return ((deQueue,haveSet,community),true) }
      // println(u1reps)
      val u2reps: RepCount = getReps(u2) getOrElse    { return ((deQueue,haveSet,community),true) }
      // println(u2reps)
    
      val u1a = u1reps.toList.sort(_._2._1 > _._2._1)
      val u2a = u2reps.toArray
      stableSort(u2a,firstGreater _)
    
      val c: Option[UserTies] = common(u1a, u2a, haveSet)
      // err.println(c)
      // val s = Set(1,2,3)
      // s flatMap (x => List((x,x),(x,x+1)))
      
      c match {
        case Some((x,ties)) => 
          val cGen = parentGen + 1
          val newQueue = 
            deQueue enqueue (community map { case (y,_,_,gen) => 
              ((y,x),gen min cGen) }) // min or max?
          // println("new queue: "+newQueue)
          val keepGoing = maxTotal.isEmpty || haveSet.size + 1 < maxTotal.get
          ((newQueue,haveSet+x,(x,parents,ties,cGen)::community),keepGoing)
        case _ => ((deQueue,haveSet,community),true)
      }
    }
    
    def growCommunity(troika: ComTroika): Community =
      addPair(troika) match {
        case (t,true) => growCommunity(t)
        case ((_,_,com),_) => com.reverse
      }
    
    // body!
    val pq: UserPairQueue = Queue.Empty.enqueue(((u1,u2),0))
    growCommunity( ( pq, Set(u1,u2), List[ComMember]((u1,(0,0),(0,0),0),(u2,(0,0),(0,0),0)) ) )
  }


  def fringeUsers(com: Community): UserSet = {
    // 2.7 has no list.toSet, mharrah suggest either:
    // Set(list:_*) or Set()++list
    val comSet: UserSet = scala.collection.immutable.Set(com:_*) map (_._1)

    // TODO how can we employ flatMap to simplify the below in 2.7?
    // in 2.8, we can use filterMap. nonEmpty, and whatnot...
    var fringeUsers = (comSet map getReps) filter (!_.isEmpty) map (Set()++_.get.keySet) map (_ -- comSet) reduceLeft (_++_)
      
    fringeUsers
  }

  def showCommunity(com: Community, csv: Boolean): String = {
    if (csv) { 
      val comSet: UserSet = scala.collection.immutable.Set(com:_*) map (_._1)
      // fun implementing mkString ourselves:
      // s.foldLeft (""){ case (x:String,i:Int) if !x.isEmpty => x+","+i; case (_,i) => i.toString }
      // s.reduceLeft( (r:Any,i:Int) => r + "," + i ).toString
      comSet mkString ","
    }
    else com.toString 
  }  
}
