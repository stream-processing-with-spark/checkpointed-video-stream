
scalaVersion := "2.11.12"
val sparkVersion = "2.4.0"

lazy val checkpointedStream = (project in file("."))
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-sql" % sparkVersion,
        "org.apache.spark" %% "spark-streaming" % sparkVersion,
        "org.scalatest" %% "scalatest" % "3.0.5" % "test"
      ),

      name := "spark-streaming-checkpoint",
      organization := "com.swas",
      mainClass in assembly := Some("com.swas.checkpointedmedia.CheckpointedMediaStream"),
      version := "1.0.1",

      scalacOptions ++= Seq(
        "-encoding", "UTF-8",
        "-target:jvm-1.8",
        "-Xlog-reflective-calls",
        "-Xlint",
        "-Ywarn-unused",
        "-Ywarn-unused-import",
        "-deprecation",
        "-feature",
        "-language:_",
        "-unchecked"
      ),

      assemblyMergeStrategy in assembly := {
        case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
        case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
        case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
        case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
        case PathList("org", "apache", xs @ _*) => MergeStrategy.last
        case PathList("com", "google", xs @ _*) => MergeStrategy.last
        case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
        case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
        case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
        case "git.properties" => MergeStrategy.last

        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      }

    )
