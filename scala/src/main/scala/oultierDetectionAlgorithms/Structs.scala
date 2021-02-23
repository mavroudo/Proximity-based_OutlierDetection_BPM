package oultierDetectionAlgorithms

import org.apache.spark.rdd.RDD

object Structs {

  case class Sequence(id: Long, events: List[Event])

  case class Event(task: String, timestamp: String, duration: Long)

  case class Log(traces: RDD[Sequence], activities: List[String])

  case class Trace_Vector(id: Long, elements: Array[Double])

  case class DistanceElement(id1: Long, id2: Long, distance: Double)

  case class DistancesFromTrace(id: Long, distances: List[DistanceElement])

  //This is for the "using contextualized"
  case class Trace_Temporal_KNN(id: Long, signature: String, durations: List[Double])
  case class Trace_Temporal(id:Long,durations:List[(String,Double)])




}
