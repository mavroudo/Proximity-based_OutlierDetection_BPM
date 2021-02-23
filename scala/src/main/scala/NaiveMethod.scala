import Utils.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
import oultierDetectionAlgorithms.Structs

case class DistanceElement(id1: Long, id2: Long, distance: Double)

case class DistancesFromTrace(id: Long, distances: List[DistanceElement])

object NaiveMethod {


  def initializeDistances(traces: RDD[Structs.Trace_Vector], k: Int): RDD[DistancesFromTrace] = {
    val spark = SparkSession.builder().getOrCreate()
    val collected = traces.collect()
    val broadcasted = spark.sparkContext.broadcast(collected)
    val distances = traces.map(v1 => {
      val distanceList = broadcasted.value.map(v2 => {
        DistanceElement(v1.id, v2.id, Utils.distanceEuclidean(v1, v2)) //TODO: change distance here
      }).toList
      DistancesFromTrace(v1.id, distanceList.sortBy(_.distance))
    })
      .map(x => {
        DistancesFromTrace(x.id, x.distances.slice(1, k + 1))
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    distances
  }

  //Slower but more efficient in big data, were even collect traces in driver is not possible
  def initialiazeDistancesRDD(traces: RDD[Structs.Trace_Vector], k: Int): RDD[DistancesFromTrace] = {
    val distances =traces.cartesian(traces)
      .map(x=>{
        DistanceElement(x._1.id,x._2.id,Utils.distance(x._1,x._2))
      })
      .groupBy(_.id1)
      .map(x=>{
        DistancesFromTrace(x._1,x._2.toList.sortBy(_.distance).slice(1,k+1))
      })
    distances.persist(StorageLevel.MEMORY_AND_DISK)
    distances
  }


}
