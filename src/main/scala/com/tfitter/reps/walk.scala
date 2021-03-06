package com.tfitter

import org.suffix.util.bdb.{BdbArgs,BdbFlags}
import com.tfitter.db.types._
import com.tfitter.db.{Twit,TwitterBDB}
import System.err
import scala.collection.mutable.{Map=>UMap}
import java.io.{ObjectOutputStream,FileOutputStream}
import java.io.{File,ObjectInputStream,FileInputStream}

// case class DialogCount1(u1: UserID, u2: UserID, n12: TwitCount) //, n21: TwitCount

@serializable
@SerialVersionUID(1L)
class Repliers {
  type RepCount = UMap[UserID,(TwitCount,TwitCount)]
  var reps: UMap[UserID, RepCount] = UMap.empty

  // TODO here goes our favorite non-autovivification,
  // a question is, whether this can be made into auto-
  def addTwit(twit: Twit): Unit = {
    twit.reply match {
      case None =>
      case Some(reply) =>
        val uid = twit.uid
        val ruid = reply.replyUser
        val tinc = if (reply.replyTwit.isEmpty) 0 else 1
        val u = reps.get(uid)
           u match {
             case Some(repcount) => repcount.get(ruid) match {
               case Some((numUser,numTwit)) => {
                 val x = reps(uid)(ruid)
                 reps(uid)(ruid) = (x._1 + 1, x._2 + tinc)
               }
               case _ => reps(uid)(ruid) = (1, tinc)
             }
             case _ => reps(uid) = UMap(ruid->(1, tinc))
           }
    }
  }

  def toPairs1 = {
    val ll = reps.toList.map {
      case (uid,repcount) => repcount.toList map {
            case (ruid,(n12,t12)) => (uid,ruid,n12,t12) }
      }
    ll.flatten
  }

  def toPairs2 = toPairs1 map { case (u1,u2,n12,t12) =>
    val (n21,t21) = reps get u2 match {
      case Some(repmap) => repmap get u1 getOrElse (0,0)
      case _ => (0,0)
    }
    val score = (n12-t12 + n21-t21) * 10 + (n12 min n21) * 100 + n12 + n21 + t12 + t21
    (u1,u2,n12,n21,t12,t21,score)
  }

  // TODO sort via array?
  def topPairs = toPairs2 sortWith (_._7 > _._7)

  def showTopPairs(n: Int) = (topPairs take n) foreach { e =>
    println("%d %d %d %d %d %d %d" format (e._1, e._2, e._3, e._4, e._5, e._6, e._7))
  }
  override def toString = reps.toString
}

object SaveRepliers {
  def main(args: Array[String]) {
    // non-transactional read-only
    val bdbFlags = BdbFlags(
      false,  // allowCreate
      true,   // readOnly
      false,  // transactional
      false,  // deferred write
      false   // noSync
    )

    val bdbEnvPath = if (args.length > 0) args(0) else Config.bdbEnvPath
    val bdbCacheSize = Some(256*1024*1024L) // None
    
    val bdbArgs = {
      import Config.{bdbStoreName}
      BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)
    }

    err.println("Twitter BDB contains the following users:")
    val tdb = new TwitterBDB(bdbArgs)
    try {
      // for (x <- tdb.allUserStats) println(x)
      // val mu: List[UserStats] = tdb.allUserStatsList
      //   println(mu mkString "\n")

      var reps = new Repliers
      for ((t,i) <- tdb.allTwits.zipWithIndex) {
        reps.addTwit(t)
        if (i % 100000 == 0) err.print('.')
      }

      if (reps.reps.size <= 100) {
        println(reps)
        println(reps.toPairs1)
        println(reps.toPairs2)
      }
      reps.showTopPairs(100)
      // TODO dump the full list into a file or BDB
      // TODO gather top 50000 different from top pairers
      val serName = "repliers.ser"
      err.print("writing pairs into "+serName+"... ")
      val oser = new ObjectOutputStream(new FileOutputStream(serName));
      oser.writeObject(reps)
      oser.close
      err.println("done")
    }
    finally {
      tdb.close
    }
  }
}


object Dessert {
  def main(args: Array[String]) {
     // to read back:
    val repSerName = "repliers.ser"
    val repFile = new File(repSerName)
    val repIn = new ObjectInputStream(new FileInputStream(repFile))
    val reps: Repliers = repIn.readObject.asInstanceOf[Repliers]
    repIn.close
     err.println("deserialized repliers:")
     // print all line by line
     // for ((u1,u2s) <- reps.reps; (u2,(t,u)) <- u2s) {
     //   println("%d->%d: %d %d" format (u1,u2,t,u))
     // }
     reps.showTopPairs(100)
   }
}


object Serialized {
  type Replist = List[(Int,Int,Int,Int,Int,Int,Int)]
  def loadRepliers(fileName: String): Repliers = {
    err.print("reading repliers from "+fileName+"... ")
    val repFile = new File(fileName)
    val repIn = new ObjectInputStream(new FileInputStream(repFile))
    val reps: Repliers = repIn.readObject.asInstanceOf[Repliers]
    repIn.close
    err.println("done")
    reps
  }
  def loadReplist(fileName: String): Replist = {
    err.print("reading top pairs from "+fileName+"... ")
    val tpFile = new File(fileName)
    val tpIn = new ObjectInputStream(new FileInputStream(tpFile))
    val tops: Replist = tpIn.readObject.asInstanceOf[Replist]
    tpIn.close
    err.println("done")
    tops
  } 
  def saveReplist(tops: Replist, fileName: String): Unit = {
    err.print("writing top pairs into "+fileName+"... ")
    val oser = new ObjectOutputStream(new FileOutputStream(fileName));
    oser.writeObject(tops)
    oser.close
    err.println("done")
  }
}
import Serialized._
 
object SaveTopPairs {
  def main(args: Array[String]) {
    val repSerName = "repliers.ser"
    val reps: Repliers = loadRepliers(repSerName)
    @serializable
    val topPairs: Replist = reps.topPairs
    val tpSerName = "toppairs.ser"
    saveReplist(topPairs,tpSerName)
  }
}


object ShowTops extends optional.Application {
  def main(n: Int, two: Option[Boolean]) {
    val tpSerName = "toppairs.ser"
    val tops: Replist  = loadReplist(tpSerName)
    val twoCols: Boolean = two getOrElse false
    if (twoCols)
      tops.take(n).foreach { x => println(x._1+" "+x._2) }
    else
      tops.take(n) foreach { e =>
        println("%d %d %d %d %d %d %d" format (e._1, e._2, e._3, e._4, e._5, e._6, e._7))
      }
  }     
}


object Triangle1 {
  def main(args: Array[String]) {
    val repSerName = "repliers.ser"
    val reps: Repliers = loadRepliers(repSerName)
    val tpSerName = "toppairs.ser"
    val tops: Replist  = loadReplist(tpSerName)
    val topN = 20
    println("got "+tops.length+" repliers, here's the first "+topN+": ")
    println(tops.take(topN))
  }   
}