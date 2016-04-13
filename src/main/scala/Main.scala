import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import java.time.{ZonedDateTime, ZoneId}

import com.google.gson.{Gson, JsonParser}

case class Tweet(createdAt: ZonedDateTime, hashtagset: Set[String])
case class HashGraph(edgesMap: Map[(String, String), ZonedDateTime], degreeMap: Map[String, Int], lowerBoundWindow: ZonedDateTime, upperBoundWindow: ZonedDateTime)

object Main {
  private var gson = new Gson()
  def flatMapSublists[A,B](ls: List[A])(f: (List[A]) => List[B]): List[B] = 
    ls match {
      case Nil => Nil
      case sublist@(_ :: tail) => f(sublist) ::: flatMapSublists(tail)(f)
    }
  
  def combinations[A](ls: List[A], n: Int): List[List[A]] =
    if (n == 0) List(Nil)
    else flatMapSublists(ls) { sl =>
      combinations(sl.tail, n - 1) map {sl.head :: _}
    }

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore _)

  def main(args: Array[String]) {
    val mappingFunc = (createdAt: ZonedDateTime, hashtags: Option[Set[String]], stateData: State[HashGraph]) => {
      val currentGraph = stateData.get()
      var HashGraph(edgesMap, degreeMap, lowerBoundWindow, upperBoundWindow) = currentGraph
      val hashtagset = hashtags.get
      if ((createdAt isAfter lowerBoundWindow) && hashtagset.size > 1) {
        // get edges
        val sortedTags = hashtagset.toList.sorted
        val edges = combinations(sortedTags, 2)
        val edgeTuples = edges.map(l => (l(0), l(1)))
        val kvs = edgeTuples.map((_ -> createdAt))
        val allEdges = edgesMap.keys.toSet
        val newEdges = edgeTuples.filter(!allEdges.contains(_))
        edgesMap = edgesMap ++ kvs
        val inc = newEdges.flatMap(x => List(x._1, x._2)).groupBy(identity).mapValues(_.size)
        // increment degree for each vertices
        degreeMap = degreeMap ++ inc.map( kv => ( kv._1 -> (degreeMap.getOrElse(kv._1, 0)+kv._2) ) )
      }
      if (createdAt isAfter upperBoundWindow) {
        upperBoundWindow = createdAt
        lowerBoundWindow = upperBoundWindow.minusSeconds(60)
        val (removedEdgesMap, updatedEdgesMap) = edgesMap.partition(p => lowerBoundWindow.compareTo(p._2) > 0)
        val removedEdges = removedEdgesMap.map(p => p._1).toList
        val dec = removedEdges.flatMap(x => List(x._1, x._2)).groupBy(identity).mapValues(_.size)
        // decrement degress for removed vertices
        degreeMap = degreeMap ++ dec.map( kv => ( kv._1 -> (degreeMap(kv._1) - kv._2) ) )
        // remove unconnected vertices
        degreeMap = degreeMap.filter(_._2 > 0)
        // remove tweets falls out of window
        edgesMap = updatedEdgesMap
      }
      val updatedGraph = HashGraph(edgesMap, degreeMap, lowerBoundWindow, upperBoundWindow)
      stateData.update(HashGraph(edgesMap, degreeMap, lowerBoundWindow, upperBoundWindow))
      if (degreeMap.size == 0) 0.0
      else degreeMap.values.foldLeft(0)(_ + _).toDouble / degreeMap.size.toDouble
    }
    
    Utils.parseCommandLineWithTwitterCredentials(args)
    val jsonParser = new JsonParser()
      
    val conf = new SparkConf().setAppName("streaming-hashgraph")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("/tmp/hashgraph-streaming");

    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth).filter(_.getHashtagEntities().length > 1)
      .map(s => (s.getCreatedAt().toInstant().atZone(ZoneId.systemDefault()), s.getHashtagEntities().map(_.getText).toSet))
    val initialGraph = HashGraph(Map[(String, String), ZonedDateTime](), Map[String, Int](), ZonedDateTime.now().minusYears(30), ZonedDateTime.now().minusYears(30).minusSeconds(60))
    //val initialRDD = ssc.sparkContext.parallelize(List(initialGraph))
    //val updatedAvgDegree = tweetStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialGraph))
    val initialRDD = ssc.sparkContext.parallelize(List(0.0))
    //val updatedAvgDegree = tweetStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))
    val updatedAvgDegree = tweetStream.mapWithState(StateSpec.function(mappingFunc))
    ssc.start()
    ssc.awaitTermination()
  }
}
