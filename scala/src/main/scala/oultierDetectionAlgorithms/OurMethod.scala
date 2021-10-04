package oultierDetectionAlgorithms

import Indexing.BallTree.BallTree
import Utils.{Preprocess, Utils, Mahalanobis}
import breeze.linalg.{DenseMatrix, inv}
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


object OurMethod {

  case class DistanceElement(id1: Long, id2: Long, distance: Double)

  case class DistancesFromTrace(id: Long, distances: List[DistanceElement])


  def findOutliers(log: Structs.Log, k: Int, r: Double, converter: Structs.Log => RDD[Structs.Trace_Vector], distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double, distanceName: String): Array[Long] = {
    val spark = SparkSession.builder().getOrCreate()
    val traces = converter(log)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    val vector_size = traces.first().elements.length
    val Row(coeff1: Matrix) = Correlation.corr(normalizedDF, "scaledFeatures").head
    val invCovariance: DenseMatrix[Double] = inv(new breeze.linalg.DenseMatrix(vector_size, vector_size, coeff1.toArray))
    val preprocessed = normalizedDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    })
    val rddDistances: RDD[DistancesFromTrace] = if (distanceName == "mahalanobis") this.initializeDistancesMahalanobis(preprocessed, k, invCovariance) else this.initializeDistances(preprocessed, k, distance)
    //normalize
//    val allDistances=rddDistances.flatMap(_.distances.map(_.distance))
//    val minDistance=allDistances.min()
//    val maxDistance=allDistances.max()
//    val normalizedDistancesRdd:RDD[DistancesFromTrace] = rddDistances
//      .map(x=>DistancesFromTrace(x.id,distances = x.distances
//        .map(y => DistanceElement(y.id1, y.id2, Utils.min_max_normalization(y.distance, minDistance, maxDistance)))))

    val outliers = rddDistances.filter(neighborhood => {
      neighborhood.distances.last.distance > r
    })
    outliers.map(_.id).collect()
  }

  def findOutliersStatistical(log: Structs.Log, k: Int, n: Double, converter: Structs.Log => RDD[Structs.Trace_Vector], distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double, distanceName: String): Array[(Long, Double)] = {
    val outlyingFactor = assignOutlyingFactor(log, k, converter, distance, distanceName)
    val mean = Utils.mean(outlyingFactor.map(_._2))
    val stdev = Utils.stdDev(outlyingFactor.map(_._2))
    outlyingFactor.filter(_._2 > mean + n * stdev)
  }

  def assignOutlyingFactor(log: Structs.Log, k: Int, converter: Structs.Log => RDD[Structs.Trace_Vector], distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double, distanceName: String): Array[(Long, Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    val traces = converter(log)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    val vector_size = traces.first().elements.length
    val Row(coeff1: Matrix) = Correlation.corr(normalizedDF, "scaledFeatures").head
    val invCovariance: DenseMatrix[Double] = inv(new breeze.linalg.DenseMatrix(vector_size, vector_size, coeff1.toArray))
    val preprocessed = normalizedDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    })
    val rddDistances: RDD[DistancesFromTrace] = if (distanceName == "mahalanobis") this.initializeDistancesMahalanobis(preprocessed, k, invCovariance) else this.initializeDistances(preprocessed, k, distance)
    val sortedByOutlyingFactor = this.assignOutlyingFactor(rddDistances, k)
    sortedByOutlyingFactor.sortBy(_._2, ascending = false).collect()
  }

  def assignOutlyingFactorWithPCA(log: Structs.Log, k: Int, dims: Int, converter: Structs.Log => RDD[Structs.Trace_Vector], distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double): Array[(Long, Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    val traces = converter(log)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    val reduceDimensionalityDF = Preprocess.reduceDimensionalityPCA(normalizedDF, dims) //apply pca
    val preprocessed = reduceDimensionalityDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("pcaFeatures").values)
    })

    val rddDistances = this.initializeDistances(preprocessed, k, distance)
    val sortedByOutlyingFactor = this.assignOutlyingFactor(rddDistances, k)
    sortedByOutlyingFactor.sortBy(_._2, ascending = false).collect()
  }

  def createBallTree(log: Structs.Log, converter: Structs.Log => RDD[Structs.Trace_Vector]): BallTree = {
    val spark = SparkSession.builder().getOrCreate()
    val traces = converter(log)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    val preprocessed = normalizedDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    })
    val ballTree: BallTree = new BallTree(preprocessed)
    ballTree.buildTree()
    ballTree
  }

  def assignOutlyingFactorWithBallTree(log: Structs.Log, ballTree: BallTree, k: Int): Array[(Long, Double)] = {
    val scores = new ArrayBuffer[(Long, Double)]()
    for (i <- 0 until log.traces.count().toInt) {
      val neighbors = ballTree.kNearestNeighbors(i.toInt, k)
      scores += ((i, neighbors.map(_._2).sum))
    }
    scores.sortWith((x, y) => x._2 > y._2).toArray
  }


  def initializeDistances(traces: RDD[Structs.Trace_Vector], k: Int, distance: (Structs.Trace_Vector, Structs.Trace_Vector) => Double): RDD[DistancesFromTrace] = {
    val spark = SparkSession.builder().getOrCreate()
    val collected = traces.collect()
    val broadcasted = spark.sparkContext.broadcast(collected)
    val distances = traces.map(v1 => {
      val distanceList = broadcasted.value.map(v2 => {
        DistanceElement(v1.id, v2.id, distance(v1, v2))
      }).toList
      DistancesFromTrace(v1.id, distanceList.sortBy(_.distance))
    })
      .map(x => {
        DistancesFromTrace(x.id, x.distances.slice(1, k + 1))
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    distances
  }

  def initializeDistancesMahalanobis(traces: RDD[Structs.Trace_Vector], k: Int, invCovariance: DenseMatrix[Double]): RDD[DistancesFromTrace] = {
    val spark = SparkSession.builder().getOrCreate()
    val collected = traces.collect()
    val broadcasted = spark.sparkContext.broadcast(collected)
    val broadcastMah = spark.sparkContext.broadcast(new Mahalanobis(invCovariance))
    val distances = traces.map(v1 => {
      val distanceList = broadcasted.value.map(v2 => {
        DistanceElement(v1.id, v2.id, broadcastMah.value.distance(v1, v2))
      }).toList
      DistancesFromTrace(v1.id, distanceList.sortBy(_.distance))
    })
      .map(x => {
        DistancesFromTrace(x.id, x.distances.slice(1, k + 1))
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    distances
  }

  private def assignOutlyingFactor(distances: RDD[DistancesFromTrace], k: Int): RDD[(Long, Double)] = {
//    var maxSize = k
//    if (k > distances.first().distances.length) {
//      maxSize = distances.first().distances.length
//    }
    distances.map(x => {
      val element: Double = x.distances.map(_.distance).sum
      (x.id, element)
    })

  }


}
