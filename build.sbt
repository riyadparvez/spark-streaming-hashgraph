lazy val root = (project in file(".")).
  settings(
    name := "spark-streaming-hashgraph",
    version := "1.0",
    scalaVersion := "2.10.6"
  )

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10"         % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.1" % "provided"
