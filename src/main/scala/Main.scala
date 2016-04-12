import org.apache.spark._
import org.apache.spark.streaming._

object Main extends App {
  val conf = new SparkConf().setAppName("streaming-hashgraph").setMaster("local[*]")
  
}
