package oultierDetectionAlgorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import Structs.{DistanceElement, DistancesFromTrace}

object InitializeNeighbors {

  def init_naive(traces:RDD[Structs.Trace_Vector],k:Int,distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double):RDD[DistancesFromTrace]={
    val spark = SparkSession.builder().getOrCreate()
    val broadcasted = spark.sparkContext.broadcast(traces.collect())
    val distances = traces.map(v1 => {
      val distanceList = broadcasted.value.map(v2 => {
        DistanceElement(v1.id, v2.id, distance(v1, v2))
      }).toList
      DistancesFromTrace(v1.id, distanceList.sortBy(_.distance))
    })
      .map(x => {
        DistancesFromTrace(x.id, x.distances.slice(0, k))
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    distances
  }

}
