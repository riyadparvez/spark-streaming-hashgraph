import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import java.io._
import java.time.{ZoneId, ZonedDateTime}

case class Tag(tag: String) extends Serializable
case class Timestamp(timestamp: ZonedDateTime) extends Serializable
case class HashGraph(lowerBound: ZonedDateTime, upperBound: ZonedDateTime, graph: Graph[Tag, Timestamp]) extends Serializable

object Main {
  import org.apache.log4j.Logger
  import org.apache.log4j.Level
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

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
    Utils.parseCommandLineWithTwitterCredentials(args)

    val conf = new SparkConf().setAppName("streaming-hashgraph")
    implicit val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("/tmp/hashgraph-streaming")

    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth).filter(_.getHashtagEntities.length > 1)
      .map(s => (s.getId, (s.getCreatedAt.toInstant.atZone(ZoneId.systemDefault()), s.getHashtagEntities.map(_.getText).toSet)))
    val vertices = sc.parallelize(Array[(VertexId, Tag)]())
    val edges = sc.parallelize(Array[Edge[Timestamp]]())
    val initialGraph = HashGraph(ZonedDateTime.now().minusYears(30), ZonedDateTime.now().minusYears(30).minusSeconds(60), Graph(vertices, edges))
    val initialRDD = ssc.sparkContext.parallelize(List((0L, initialGraph)))

    val mappingFunc = (batchTime: Time, id: Long, value: Option[(ZonedDateTime, Set[String])], stateData: State[HashGraph]) => {
      val currentGraph = stateData.getOption.getOrElse(initialGraph)
      //val currentGraph = stateData.getOption.get
      val t = value.getOrElse((ZonedDateTime.now().minusYears(30), Set[String]()))
      val createdAt = t._1
      val hashtagset = t._2
      var HashGraph(lowerBound, upperBound, rawGraph) = currentGraph
      var updatedGraph = rawGraph

      if ((createdAt isAfter lowerBound) && hashtagset.size > 1) {
        // get edges
        val sortedTags = hashtagset.toList.sorted
        val edges = combinations(sortedTags, 2)
        //val edgesRDD = sc.parallelize(edges.map(l => Edge(l(0).hashCode, l(1).hashCode, Timestamp(createdAt))))
        val edgesRDD = edges.map(l => Edge(l(0).hashCode, l(1).hashCode, Timestamp(createdAt)))

        //updatedGraph = Graph(rawGraph.vertices, rawGraph.edges ++ edgesRDD)
        //val allEdges = edgesMap.keys.toSet
      }
      if (createdAt isAfter upperBound) {
        upperBound = createdAt
        lowerBound = upperBound.minusSeconds(60)
        //val (removedEdgesMap, updatedEdgesMap) = edgesMap.partition(p => lowerBound.compareTo(p._2) > 0)
        //val removedEdges = removedEdgesMap.keys.toList
      }
      val updatedHashGraph = HashGraph(lowerBound, upperBound, rawGraph)
      stateData.update(updatedHashGraph)
      Option(0.0)
    }

    val updatedAvgDegree = tweetStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))
    updatedAvgDegree.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
