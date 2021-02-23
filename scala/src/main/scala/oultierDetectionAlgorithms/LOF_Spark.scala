package oultierDetectionAlgorithms

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


/**
 * Total complexity of this method to find outlier score is O(n2+n2k2d), where n is the number of traces,
 * k is the number of neighbors (minPts) and d is the number of dimensions.
 * Even though this parallelism was used with the help of spark, it is hard to apply it on real-life scenarios
 * @param traces The traces after preprocess
 * @param minPts Number of neighbors take into consideration
 * @param distance Distance that will be applied on the Vectors
 */
class LOF_Spark(traces: RDD[Structs.Trace_Vector], minPts: Int, distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double) {
  private val spark = SparkSession.builder().getOrCreate()
  private var distances_from_k: RDD[Structs.DistancesFromTrace] = _
  private var neighbors: RDD[(Long, List[(Long, Double, Double, Double)])] = _
  private var broadMinPts: Broadcast[Int] = _
  private var lrds: RDD[(Long, Double)] = _

  def initialize(init_method: (RDD[Structs.Trace_Vector], Int, (Structs.Trace_Vector, Structs.Trace_Vector) => Double) => RDD[Structs.DistancesFromTrace]): Unit = {
    distances_from_k = init_method(traces, minPts, distance)
    val k_distances = distances_from_k.collect().map(x => (x.id, x.distances(minPts - 1).distance))
    broadMinPts = spark.sparkContext.broadcast(minPts)
    neighbors = distances_from_k.map(x => { // id, distance, k.distance(o)
      val id = x.id
      val innerk_distances = k_distances.filter(m => x.distances.map(_.id2).contains(m._1))
      val ds = x.distances.map(d => {
        (d.id2, d.distance, innerk_distances.filter(_._1 == d.id2).head._2, math.max(d.distance, innerk_distances.filter(_._1 == d.id2).head._2))
      })
      (id, ds)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    lrds = neighbors.map(x => { // compute lrds without 1/ and |number of neighbors|
      val sumComponent = x._2.map(_._4).sum
      (x._1, sumComponent)
    }).persist(StorageLevel.MEMORY_AND_DISK)


  }


  def loFactor(trace: Structs.Trace_Vector): Double = {
    val nn = neighbors.filter(_._1 == trace.id).first()._2
    val thisLrd = lrds.filter(_._1 == trace.id).first()
    lrds.filter(x => nn.map(_._1).contains(x._1)) //lrds that we need
      .map(_._2 / thisLrd._2)
      .sum() / broadMinPts.value
  }


  def assignOutlierFactor(): Array[(Long,Double)] = {
    traces.collect().map(x => {
      (x.id,this.loFactor(x))
    })
  }

  def unpersist():Unit={
    this.neighbors.unpersist(true)
    this.lrds.unpersist(true)
  }

}
