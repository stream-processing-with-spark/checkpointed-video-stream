package com.swas.checkpointedmedia

import java.sql.Timestamp

import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object CheckpointedMediaStream {

  //
  val MillisInAnHour = Minutes(60).milliseconds
  val hourFromTimestamp: Long => Long = timestamp =>  timestamp - timestamp % MillisInAnHour

  // definition of the state operation
  def trackVideoHits(videoId: String, timestamp:Option[Long], runningCount: State[VideoPlayCount]): Option[VideoPlayCount] = {
    val oldCount = runningCount.getOption.getOrElse(VideoPlayCount(videoId, hourFromTimestamp(timestamp.getOrElse(0L)), 0L))
    val currentTimestamp = timestamp.getOrElse(0L)
    // if the reading falls within the hour, we update the reading and do not return a result
    if (currentTimestamp < oldCount.day + MillisInAnHour) {
      val newCount = oldCount.copy(count = oldCount.count + 1)
      runningCount.update(newCount)
      None
    } else { // if the reading falls beyond the hour, we report the count of the previous our and update the state with the new hour
      val newHourlyCount = VideoPlayCount(videoId, hourFromTimestamp(timestamp.getOrElse(0)), 1)
      runningCount.update(newHourlyCount)
      Some(oldCount)
    }
  }

  // method to create the streaming context and setup the process
  def setupContext(checkpointDir : String, sparkContext: SparkContext): StreamingContext = {
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))
    sparkContext.setLogLevel("WARN")
    // sets the checkpoint directory
    streamingContext.checkpoint(checkpointDir)

    // This setup is only for local running.
    // In particular, the storage level is set for no replication.
    val stream  = streamingContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)
    // print the amount of data received on each micro-batch
    stream.count()

    // Materialize the data function generator
    val videoPlayedDStream  = stream.flatMap(textLine => VideoPlayed.fromCsv(textLine))

    val checkpointedVideoPlayedDStream = videoPlayedDStream.checkpoint(Seconds(60))

    import org.apache.spark.streaming._
    val videoHitsCounterSpec = StateSpec.function(trackVideoHits _).timeout(Seconds(3600))

    // To use mapWithState, we need a (Key,Value) structure.
    // In our case, we use (videoId, timestamp)
    val videoHitsByTimestamp = checkpointedVideoPlayedDStream.map(videoPlay => (videoPlay.videoId, videoPlay.timestamp))

    // Stateful stream of videoHitsPerHour
    val statefulVideoHitsPerHour = videoHitsByTimestamp.mapWithState(videoHitsCounterSpec)

    // remove the None values from the state stream by "flattening" the DStream
    val videoHitsPerHour = statefulVideoHitsPerHour.flatMap(elem => elem)

    // print the top-10 highest values
    videoHitsPerHour.foreachRDD{ ( rdd, time ) =>
      val top10 = rdd.top(10)(Ordering[Long].on((v: VideoPlayCount) => v.count))
      val currentTimestamp = new Timestamp(time.milliseconds)
      println(s"Top 10 at time $currentTimestamp")
      println("=========================")
      top10.foreach(videoCount => println(videoCount))
      println("=========================")
    }
    // return the created streaming context
    streamingContext
  }

  def main(args: Array[String]): Unit = {

    // this is our checkpoint location. In a cluster, set it to a location reachable from the driver and executors
    val CheckpointDir = "/tmp/streaming"

    // start the built-in TCP server
    import scala.concurrent.ExecutionContext.Implicits.global
    DataGenerators.builtInTCPServer()

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("checkpointed-media-stream")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Recreate the StreamingContext from if a checkpoint exists or create a new one
    val streamingContext = StreamingContext.getOrCreate(CheckpointDir, () => setupContext(CheckpointDir, spark.sparkContext))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

