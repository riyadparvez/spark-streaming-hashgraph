lazy val root = (project in file(".")).
  settings(
    name := "spark-streaming-hashgraph",
    version := "1.0",
    scalaVersion := "2.10.6"
  )

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

libraryDependencies += "commons-io" % "commons-io" % "2.4"
libraryDependencies += "com.google.code.gson" % "gson" % "2.3"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"
//libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10"         % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10"         % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.1"
