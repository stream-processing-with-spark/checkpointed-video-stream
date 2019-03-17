package com.swas.checkpointedmedia

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

// definitions of our data representation for the VideoPlayed data point and the Video Play Count
case class VideoPlayed(videoId: String, clientId: String, timestamp: Long) {
 def toCsv: String = s"$videoId, $clientId, $timestamp"
}
object VideoPlayed extends Serializable {
  def fromCsv(str: String) : Option[VideoPlayed] = {
    Try{
      val Array(id, client, ts) = str.split(",")
      VideoPlayed(id, client, ts.toLong)
    }.toOption
  }
}

case class VideoPlayCount(videoId: String, day: Long, count: Long)

object DataGenerators extends Serializable {

  val ServerPort = 9999

  // Some ad-hoc data to generate random data points
  val videos = (1 to 1000).map(i => s"video-$i")
  val clients = (1 to 100000).map(i => s"user-$i")

  import scala.util.Random
  // generic function to get a random value out of a sequence
  val randomValueFrom: Seq[String] => String = values => values(Random.nextInt(values.size))
  // get a random video
  val randomVideo: () => String = () => randomValueFrom(videos)
  val randomUser: () => String = () => randomValueFrom(clients)
  // videoHit generating function for a random number of hits
  val randomVideoHits: () => Seq[VideoPlayed] = () => {
    (1 to Random.nextInt(1000)).map{
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
          println(s"Delivering ${videoHits.size} video hits")
          videoHits.foreach(hit => out.println(hit.toCsv))
          out.flush()
        }
      }
    }

    def deliverData(socket: Socket): Future[Unit] = {
      val out = new PrintStream(socket.getOutputStream())
      deliver(out)
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
