package com.epam.spark

import com.restfb.batch.{BatchRequest, BatchResponse}
import com.restfb.batch.BatchRequest.BatchRequestBuilder
import com.restfb._
import com.restfb.json.JsonObject

import scala.collection.JavaConversions._
import com.restfb.types.{Event, Location, Place, User}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.util.Random

/**
 * Hello world!
 *
 */
object App {

  val ACCESS_TOKE = "701119916692700|KqXQ8wZJ30p4Va7WI4_m55tpAkk"
  val DIGIT_REGEXP = "[0-9]".r
  val MAX_REQUEST_BATCH_SIZE = 50

  def main(args: Array[String]): Unit = {
    val name = args(0)
    val master = args(1)
    val tagsFilePath = args(2)
    val citiesFilePath = args(3)
    val inputFilesPath = args(4)
    val tagsCount = args(5).toInt
    val tagsInfoOutputPath = args(6)
    val usersInfoOutputPath = args(7)


    val conf = new SparkConf().setAppName(name).setMaster(master)
    val sc = new SparkContext(conf)

    //tags dic broadcast variable creation
    val tagsFile = sc.textFile(tagsFilePath)
    val filteredTagsData = tagsFile.filter(line => !line.startsWith("ID"))
    val idToTags = filteredTagsData.map(line => {
      val splits = line.split('\t'); (splits(0), splits(1).split(',').toSet)
    }).collectAsMap()
    val idToTagsMap = sc.broadcast(idToTags)

    //cities dic broadcast variable creation
    val citiesFile = sc.textFile(citiesFilePath)
    val filteredCitiesData = citiesFile.filter((line => !line.startsWith("Id")))
    val idToCityInfo = filteredCitiesData.map(line => {
      val splits = line.split('\t'); (splits(0), (splits(1), splits(4).toDouble, splits(6).toDouble, splits(7).toDouble))
    }).collectAsMap()
    val idToCityInfoMap = sc.broadcast(idToCityInfo)

    //make RDD from clicks dataset
    val clicksInfo = sc.textFile(inputFilesPath)
    //map line to tuple ((date, city_id) -> tagsSet)
    val uniqueTagsPerDayPerCity = clicksInfo
      .map(line => {
        val splits = line.split('\t')
        ((splits(1).substring(0, 8), splits(6)), idToTagsMap.value.getOrElse(splits(20), Set.empty[String]))
      })
      //filter tuples with empty tags or missing city id
      .filter { case (x, y) => !idToCityInfo.get(x._2).isEmpty && !y.isEmpty}
      //agregate tags by date and place
      .reduceByKey((x, y) => x ++ y)

    //make maps by partition to prevent too many facebook clients
    val kwEventToAttending = uniqueTagsPerDayPerCity.mapPartitions(lines => {
        val facebookClient = new DefaultFacebookClient(ACCESS_TOKE, Version.VERSION_2_6)

        //map tuple to tuple ((tag, city_id, Event) -> List[User])
        lines.toList.flatMap { case (x, y) => {
          val cityInfo = idToCityInfo.get(x._2).get
          val cityName = cityInfo._1
          val lat = cityInfo._3
          val long = cityInfo._4
          val r = 5000

          //take only first 10 tags)
          val tags = Random.shuffle(y.filter(keyword => DIGIT_REGEXP.findFirstIn(keyword).isEmpty)).take(tagsCount)

          //make request for places by coordinates and tag
          val reqList = mutable.ListBuffer[BatchRequest]()
          tags.foreach(tag => {
            val req = new BatchRequestBuilder("search")
              .parameters(
                Parameter.`with`("q", tag),
                Parameter.`with`("type", "place"),
                Parameter.`with`("fields", "id"),
                Parameter.`with`("center", lat + "," + long),
                Parameter.`with`("distance", r),
                Parameter.`with`("limit", 50))
              .build()
            reqList += req
          })
          //make batch request
          val placeResps = facebookClient.executeBatch(reqList)
          //zip tag with appropriate response
          val tagToPlaceRespMap = tags.zip(placeResps)

          //create map from tag to list of places ids
          val tagToPlacesIds = mutable.Map.empty[String, mutable.ListBuffer[String]].withDefaultValue(mutable.ListBuffer.empty)
          for ((tag, placeResp) <- tagToPlaceRespMap) {
            if (placeResp.getCode == 200) {
              val places = new Connection(facebookClient, placeResp.getBody, classOf[Place])
              //take only first page of places
              places.getData.foreach(place => {
                val ids = tagToPlacesIds(tag)
                ids += place.getId
                tagToPlacesIds(tag) = ids
              })
            } else {
              println("Error response: " + placeResp)
            }
          }

          //create map from tuple (tag, city_id, Event) to list of event attending
          val kwToEventMap = mutable.HashMap[Tuple3[String, String, Event], List[User]]()
          if (!tagToPlacesIds.isEmpty) {
            tagToPlacesIds.foreach{ case (tag, idList) => {
              idList.grouped(MAX_REQUEST_BATCH_SIZE).foreach(group => {
                val eventReqs = mutable.ListBuffer[BatchRequest]()
                for (id <- group) {
                  //request for events of place with specific keyword
                  eventReqs += new BatchRequestBuilder( id + "/events").parameters(
                    Parameter.`with`("q", tag),
                    Parameter.`with`("since", "1463560287"),
                    Parameter.`with`("until", "1467102777"),
                    Parameter.`with`("fields", "attending_count,id,start_time,description"),
                    Parameter.`with`("limit", 10)
                  ).build()
                }

                //execute batch request to facebook
                val eventResps = facebookClient.executeBatch(eventReqs)
                for (eventResp <- eventResps) {
                  if (eventResp.getCode == 200) {
                    val events = new Connection(facebookClient, eventResp.getBody, classOf[Event])
                    for (event <- events.getData) {
                      if (event.getAttendingCount > 0) {
                        //fetch event attending
                        val attending = facebookClient.fetchConnection(event.getId + "/attending", classOf[User])
                        kwToEventMap((tag, cityName, event)) = attending.getData.toList
                      }
                    }
                  } else {
                    println("Error resp: " + eventResp)
                  }
                }

              })
            }}
          }

          kwToEventMap.toList

        }}.iterator
      }
    ).cache()

    //write to file tuples (tag, event_start_date, city_name, attending_count, token_map)
    kwEventToAttending.map(m => {
        val key = m._1
        val value = m._2

        val event = key._3
        val city = key._2
        val kw = key._1

        val attending = value

        val description = event.getDescription
        val tokenCountMap = mutable.Map.empty[String, Int].withDefaultValue(0)
        for (rawWord <- description.split("[,!.?\\s]+")) {
          val word = rawWord.toLowerCase
          tokenCountMap(word) += 1
        }

        val sortedTokens = tokenCountMap.toList.sortWith(_._2 > _._2)
        (kw, event.getStartTime.getTime.toString, city, event.getAttendingCount, sortedTokens.take(10).mkString(":"))
    }).saveAsTextFile(tagsInfoOutputPath)

    //write to file users with number of occurrences sorted by
    kwEventToAttending
      .flatMap(m => m._2)
      .map(user => (user.getName, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)
      .saveAsTextFile(usersInfoOutputPath)

  }
}
