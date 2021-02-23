package oultierDetectionAlgorithms

import Utils.Preprocess
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Structs.Trace_Temporal_KNN

import scala.math.Ordered.orderingToOrdered

object ODAL {

  def find_outliers(log: Structs.Log, k: Int, n: Float): Array[Long] = {
    val preprocessedRDD: RDD[Structs.Trace_Temporal_KNN] = log.traces.map(trace => {
      val signature = new StringBuilder()
      trace.events.foreach(event => {
        signature.append(event.task)
      })
      Structs.Trace_Temporal_KNN(trace.id, signature.toString(), trace.events.map(_.duration.toDouble))
    })
    val spark = SparkSession.builder().getOrCreate()
    val traces = spark.sparkContext.broadcast(preprocessedRDD.collect())
    val kthNeighbor = preprocessedRDD.map(trace => {
      val distances = traces.value.filter(_.signature == trace.signature)
        .map(x => this.distance(x, trace))
        .sorted
      val point = Math.min(k + 1, distances.length - 1)
      (trace, distances(point), point)
    })
    val distances = kthNeighbor.map(_._2).collect()
    val mean_value = mean[Double](distances)
    val stdev_value = stdDev[Double](distances)
    kthNeighbor.filter(x => x._2 > mean_value + n * stdev_value)
      .map(_._1.id)
      .collect()

  }

  def findOutliersAlignment(log: Structs.Log, k: Int, n: Float): Array[Long] = {
    val spark = SparkSession.builder().getOrCreate()
    val preprocessedRDD = log.traces.map(trace => {
      val l = trace.events.map(x => (x.task, x.duration.toDouble))
      Structs.Trace_Temporal(trace.id, l)
    })
    val traces = spark.sparkContext.broadcast(preprocessedRDD.collect())
    val kthNeighbor = preprocessedRDD.map(trace => {
      val distances = traces.value
        .map(x => this.distanceAlign1by1(x, trace))
        .sorted
      val point = Math.min(k + 1, distances.length - 1)
      (trace, distances(point), point)
    })
    val distances = kthNeighbor.map(_._2).collect()
    val mean_value = mean[Double](distances)
    val stdev_value = stdDev[Double](distances)
    kthNeighbor.filter(x => x._2 > mean_value + n * stdev_value)
      .map(_._1.id)
      .collect()
  }

  /**
   * The have the same signature
   *
   * @param trace1
   * @param trace2
   * @return
   */
  private def distance(trace1: Trace_Temporal_KNN, trace2: Trace_Temporal_KNN): Double = {
    trace1.durations.zip(trace2.durations)
      .map(x => Math.abs(x._1 - x._2))
      .sum
  }

  /**
   * With everyone and consider 0 at the activities that are not the same
   *
   * @param trace1
   * @param trace2
   * @return
   */
  private def distanceAlign1by1(trace1: Structs.Trace_Temporal, trace2: Structs.Trace_Temporal): Double = {
    val l1 = trace1.durations.size
    val l2 = trace2.durations.size
    var trace_min = trace2
    var trace_max = trace1
    if (l1 < l2) {
      trace_min = trace1
      trace_max = trace2
    }
    trace_min.durations.zip(trace_max.durations)
      .filter(x=>x._1._1==x._2._1)
      .map(x=>Math.abs(x._1._2-x._2._2))
      .sum
  }


  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

}
