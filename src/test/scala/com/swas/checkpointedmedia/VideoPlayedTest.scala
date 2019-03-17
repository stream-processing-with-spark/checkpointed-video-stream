package com.swas.checkpointedmedia

import org.scalatest.{Matchers, OptionValues, WordSpec}

class VideoPlayedTest extends WordSpec with Matchers with OptionValues {

  "VideoPlayed" should {
    "convert to a from a csv format" in {
      val sample = "videoid, userid, 1552877134159"
      val videoPlayed = VideoPlayed.fromCsv(sample)
      videoPlayed.value.videoId should be ("videoid")
      videoPlayed.value.clientId should be ("userid")
      videoPlayed.value.timestamp should be (1552877134159L)
    }
    "have a round-trip conversion" in {
      val sample = VideoPlayed("videoid", "userid", 1552877134159L)
      VideoPlayed.fromCsv(sample.toCsv).value should be (sample)
    }

  }


}
