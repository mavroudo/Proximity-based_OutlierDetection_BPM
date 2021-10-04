package oultierDetectionAlgorithms

import Utils.{Mahalanobis, Preprocess, Utils}
import breeze.linalg.{DenseMatrix, inv}
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import oultierDetectionAlgorithms.Structs.Trace_Vector

import scala.collection.mutable


object LOF_core {

  def assignOutlierFactor(log: Structs.Log, k: Int, converter: Structs.Log => RDD[Structs.Trace_Vector], distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double, distanceName: String): Array[(Long, Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    val traces = this.preprocess(log, converter)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "scaledFeatures")
    val vector_size = traces.first().elements.length
    val Row(coeff1: Matrix) = Correlation.corr(df, "scaledFeatures").head
    val invCovariance: DenseMatrix[Double] = inv(new breeze.linalg.DenseMatrix(vector_size, vector_size, coeff1.toArray))
    val KNeighborhood = if (distanceName == "mahalanobis") this.findNeighborhoodMahalanobis(traces, k, invCovariance) else this.findNeighborhood(traces, k, distance)
    val kDistanceMap = new mutable.HashMap[Long, Double]()
    KNeighborhood.collect().foreach(neighborhood => {
      kDistanceMap(neighborhood.id) = this.k_distance(neighborhood, k)
    })
    val bcKDistanceMap = spark.sparkContext.broadcast(kDistanceMap)
    val lrd_k: RDD[(Long, Double)] = KNeighborhood.map(kneighborhood => {
      val sumReachDistance = kneighborhood.distances.map(neighbor => {
        Math.max(neighbor.distance, bcKDistanceMap.value(neighbor.id2))
      }).sum
      if (sumReachDistance == 0) (kneighborhood.id, 0) else (kneighborhood.id, 1 / (sumReachDistance / k))
    })
    val lrdKMap = new mutable.HashMap[Long, Double]()
    lrd_k.collect().foreach(lrdk => {
      lrdKMap(lrdk._1) = lrdk._2
    })
    val bclrdkMap = spark.sparkContext.broadcast(lrdKMap)
    val lofScore = KNeighborhood.map(kneighborhood => {
      val sumOfLrds = kneighborhood.distances.map(neihgbor => {
        bclrdkMap.value(neihgbor.id2)
      }).sum
      val thisLRD = bclrdkMap.value(kneighborhood.id)
      (kneighborhood.id, sumOfLrds / (k * thisLRD))
    })
    val scores = lofScore.sortBy(_._2, ascending = false).collect()
    scores
  }


  //  def assignScoreOutlierEvents(log: Structs.Log, k: Int, converter: Structs.Log => RDD[Structs.Trace_Vector]): Unit = {
  //    val spark = SparkSession.builder().getOrCreate()
  //    val traces = log.traces.flatMap(trace => {
  //      trace.events.zipWithIndex.map(event => {
  //        Event(trace.id, event._1.task, event._1.duration, event._2)
  //      })
  //    })
  //    val perActivities = traces.groupBy(_.task)
  //    val neighbors = perActivities
  //      .map(activity => {
  //        val neighbors = activity._2.map(event => {
  //          val distances = activity._2.map(i_ev => {
  //            (i_ev.trace_index, i_ev.event_pos, Math.abs(i_ev.duration - event.duration))
  //          }).toList.sortWith((x, y) => x._3 < y._3).slice(1, k + 1)
  //          (event, distances)
  //        })
  //        (activity._1,neighbors)
  //      })
  //  }

  private def preprocess(log: Structs.Log, converter: Structs.Log => RDD[Structs.Trace_Vector]): RDD[Trace_Vector] = {
    val spark = SparkSession.builder().getOrCreate()
    val transformed: RDD[Trace_Vector] = converter(log)
    val preparedForRdd = transformed.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    normalizedDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    })
  }

  private def findNeighborhood(traces: RDD[Trace_Vector], k: Int, distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double): RDD[Structs.DistancesFromTrace] = {
    val spark = SparkSession.builder().getOrCreate()
    val collected = traces.collect()
    val broadcasted = spark.sparkContext.broadcast(collected)
    val distances = traces.map(v1 => {
      val distanceList = broadcasted.value.map(v2 => {
        Structs.DistanceElement(v1.id, v2.id, distance(v1, v2))
      }).toList
      Structs.DistancesFromTrace(v1.id, distanceList.sortBy(_.distance))
    })
      .map(x => {
        Structs.DistancesFromTrace(x.id, x.distances.slice(1, k + 1))
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    distances
  }

  private def findNeighborhoodMahalanobis(traces: RDD[Trace_Vector], k: Int, invCovariance: DenseMatrix[Double]): RDD[Structs.DistancesFromTrace] = {
    val spark = SparkSession.builder().getOrCreate()
    val collected = traces.collect()
    val broadcasted = spark.sparkContext.broadcast(collected)
    val broadcastMah = spark.sparkContext.broadcast(new Mahalanobis(invCovariance))
    val distances = traces.map(v1 => {
      val distanceList = broadcasted.value.map(v2 => {
        Structs.DistanceElement(v1.id, v2.id, broadcastMah.value.distance(v1, v2))
      }).toList
      Structs.DistancesFromTrace(v1.id, distanceList.sortBy(_.distance))
    })
      .map(x => {
        Structs.DistancesFromTrace(x.id, x.distances.slice(1, k + 1))
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    distances
  }

  private def k_distance(o: Structs.DistancesFromTrace, k: Int): Double = {
    o.distances(k - 1).distance
  }


  case class Event(trace_index: Long, task: String, duration: Double, event_pos: Long)
  def findOutlierEvents(log: Structs.Log, k: Int, zeta: Int): Array[(Event, Double)] = {
    log.traces.zipWithIndex.flatMap(trace => {
      val trace_index = trace._2
      trace._1.events.zipWithIndex.map(event => {
        val event_pos = event._2
        Event(trace_index, event._1.task, event._1.duration, event_pos)
      })
    }).groupBy(_.task)
      .flatMap(x => {
        this.lof_per_activity(x._2, k)
      })
      .sortBy(_._2,ascending = false)
      .take(zeta)

  }

  def lof_per_activity(events: Iterable[Event], k: Int):List[(Event,Double)] = {
    //find neighborhood
    case class DistanceEvents(e1: Event, e2: Event, distance: Double)
    case class DistanceListEvent(e1: Event, l: List[DistanceEvents])
    val distances = events.map(e1 => {
      val l = events.map(e2 => {
        DistanceEvents(e1, e2, Math.abs(e1.duration - e2.duration))
      }).toList.sortBy(_.distance).slice(1, k + 1)
      DistanceListEvent(e1, l)
    })
    //create hashmap for fast looking k distance
    val kDistanceMap = new mutable.HashMap[(Long, Long), Double]()
    distances.foreach(event => {
      val lastOne= if (event.l.length<k-1) event.l.length-1 else k-1
      kDistanceMap((event.e1.trace_index, event.e1.event_pos)) = event.l(lastOne).distance
    })
    //create lrdk and hashmap for fast searching
    val lrd_k = distances.map(event => {
      val sumReachDistance = event.l.map(e2 => {
        Math.max(e2.distance, kDistanceMap((e2.e2.trace_index, e2.e2.event_pos)))
      }).sum
      if (sumReachDistance == 0) (event.e1, 0.0) else (event.e1, 1 / (sumReachDistance / k))
    })
    val lrdKMap = new mutable.HashMap[(Long, Long), Double]()
    lrd_k.foreach(l => {
      lrdKMap((l._1.trace_index, l._1.event_pos)) = l._2
    })
    // define score for every event
    val lofScore = distances.map(event => {
      val sumOfLrds = event.l.map(e2 => {
        lrdKMap(e2.e2.trace_index, e2.e2.event_pos)
      }).sum
      val thisLRD = lrdKMap((event.e1.trace_index, event.e1.event_pos))
      (event.e1, sumOfLrds / (k * thisLRD))
    })
    //min max normalization so top zeta will work
    val maxScore = lofScore.toList.map(_._2).max
    val minScore = lofScore.toList.map(_._2).min
    lofScore.map(x=>(x._1,Utils.min_max_normalization(x._2,minScore,maxScore))).toList
  }

}
