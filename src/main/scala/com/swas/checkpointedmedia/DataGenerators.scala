package com.swas.checkpointedmedia

import java.sql.Timestamp

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

// definitions of our data representation for the VideoPlayed data point and the Video Play Count
case class VideoPlayed(videoId: String, clientId: String, timestamp: Long) {
 def toCsv: String = s"$videoId, $clientId, $timestamp"
}
object VideoPlayed extends Serializable {
  def fromCsv(str: String) : Option[VideoPlayed] = {
    Try{
      val Array(id, client, ts) = str.split(",").map(_.trim)
      VideoPlayed(id, client, ts.toLong)
    }.toOption
  }
}

case class VideoPlayCount(videoId: String, day: Long, count: Long) {
  override def toString = {
    val ts = new Timestamp(day)
    s"$videoId, $ts, $count"
  }
}

object DataGenerators extends Serializable {

  val ServerPort = 9999
  val TotalVideoIds = 1000
  val TotalUserIds = 100000
  val MaxVideoHitsGenerated = 250
  val MaxGenerationDelayMs = 100

  // Some ad-hoc data to generate random data points
  val videos = (1 to TotalVideoIds).map(i => s"video-$i")
  val clients = (1 to TotalUserIds).map(i => s"user-$i")

  import scala.util.Random
  // generic function to get a random value out of a sequence
  val randomValueFrom: Seq[String] => String = values => values(Random.nextInt(values.size))
  // get a random video
  val randomVideo: () => String = () => randomValueFrom(videos)
  val randomUser: () => String = () => randomValueFrom(clients)
  // videoHit generating function for a random number of hits
  val randomVideoHits: () => Seq[VideoPlayed] = () => {
    (1 to Random.nextInt(MaxVideoHitsGenerated)).map{
      i => VideoPlayed(randomVideo(), randomUser(), System.currentTimeMillis + i*60000)
    }
  }

  def builtInTCPServer()(implicit ex: ExecutionContext): Future[Unit] = {
    import java.net._
    import java.io._

    def deliver(out: PrintStream): Future[Unit] = {
      Future {
        while (!out.checkError()) {
          val videoHits = randomVideoHits()
          videoHits.foreach(hit => out.println(hit.toCsv))
          Thread.sleep(Random.nextInt(MaxGenerationDelayMs).toLong)
          out.flush()
        }
      }
    }

    def deliverData(socket: Socket): Future[Unit] = {
      println(s"Connected to client $socket")
      deliver(new PrintStream(socket.getOutputStream()))
    }

    Future {
      println("TCP Server started!")
      val server = new ServerSocket(ServerPort)
      while (true) {
        deliverData(server.accept())
      }
    }
  }
}
