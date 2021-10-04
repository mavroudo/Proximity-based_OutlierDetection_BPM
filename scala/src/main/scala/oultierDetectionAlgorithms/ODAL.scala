package oultierDetectionAlgorithms

import Utils.Preprocess
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Structs.{Log, Trace_Temporal_KNN}

import scala.collection.mutable.ListBuffer
import scala.math.Ordered.orderingToOrdered

object ODAL {

  def find_outliers_admition(log: Structs.Log, k: Int, n :Float):Array[Long] = {
    val preprocessedRDD: RDD[Structs.Transformation] = log.traces.map(trace => {
      val durations = new Array[ListBuffer[Double]](log.activities.length)
      for(i<-durations.indices){
        durations(i)=new ListBuffer[Double]()
      }
      trace.events.foreach(event => {
        durations(log.activities.indexOf(event.task))+=event.duration
      })
      val signature = durations.map(x=>x.nonEmpty).toList
      Structs.Transformation(trace.id, durations,signature)
    })
    val spark = SparkSession.builder().getOrCreate()
    val traces = spark.sparkContext.broadcast(preprocessedRDD.collect())
    val kthNeighbor = preprocessedRDD.map(trace => {
      val distances = traces.value.filter(_.signature == trace.signature)
        .map(x => this.distance_admition(x, trace))
        .sorted
      val point = Math.min(k + 1, distances.length - 1)
      (trace, distances(point), distances, point, trace.signature)
    })
    val neighbors = kthNeighbor.map(x=>(x._1.id,x._3.length)).sortBy(_._2,ascending = false)
    val outliers = kthNeighbor
      .groupBy(_._5)
      .flatMap(x=>{
        val distances = x._2.map(_._2)
        val mean_value = mean[Double](distances)
        val stdev_value = stdDev[Double](distances)
        x._2.filter(y=>{
          y._2> mean_value+n*stdev_value
        })
      })
      .map(_._1.id).collect()
    outliers


//    val outliers = kthNeighbor
//      .filter(x => {
//        val mean_value = mean[Double](x._3)
//        val stdev_value = stdDev[Double](x._3)
//        x._2 > mean_value + n * stdev_value
//      })
//    val ids=outliers
//      .map(_._1.id)
//      .collect()
//        ids
  }

  private def distance_admition(t1:Structs.Transformation,t2:Structs.Transformation):Double={
    Math.sqrt(t1.durations.zip(t2.durations)
      .filter(y=>y._2.nonEmpty && y._1.nonEmpty) // remove empty
      .map(x=>{
        val smaller= if (x._1.size>x._2.size) (x._2,x._1) else (x._1,x._2)
        var sum:Double = 0
        for(i <-smaller._1.indices){
          sum += Math.pow(smaller._1(i)-smaller._2(i),2)
        }
        sum
      }).sum)
  }

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
      (trace, distances(point), distances, point)
    })
    val neighbors = kthNeighbor.map(x=>(x._1.id,x._3.length)).sortBy(_._2,ascending = false)
    val outliers = kthNeighbor
      .filter(x => {
        val mean_value = mean[Double](x._3)
        val stdev_value = stdDev[Double](x._3)
        x._2 > mean_value + n * stdev_value
      })
      val ids=outliers
      .map(_._1.id)
      .collect()

    ids
    //    val distances = kthNeighbor.map(_._2).collect()
    //    val mean_value = mean[Double](distances)
    //    val stdev_value = stdDev[Double](distances)
    //    kthNeighbor.filter(x => x._2 > mean_value + n * stdev_value)
    //      .map(_._1.id)
    //      .collect()

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
      .map(x => Math.pow(Math.abs(x._1 - x._2), 2))
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
      .filter(x => x._1._1 == x._2._1)
      .map(x => Math.abs(x._1._2 - x._2._2))
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
