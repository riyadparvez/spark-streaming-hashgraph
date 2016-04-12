import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import com.google.gson.Gson

object Main {
  private var gson = new Gson()

  def main(args: Array[String]) {
    Utils.parseCommandLineWithTwitterCredentials(args)
      
    val conf = new SparkConf().setAppName("streaming-hashgraph")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(60))
  
    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth)
      .map(gson.toJson(_))
    
    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(4)
        outputRDD.saveAsTextFile("/tmp" + "/tweets_" + time.milliseconds.toString)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
