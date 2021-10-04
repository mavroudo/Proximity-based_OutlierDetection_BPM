import Indexing.BallTree.BallTree
import Utils.{Preprocess, Results}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.{OurMethod, Structs}

import scala.collection.mutable.ListBuffer

class NaiveMethodTest extends FunSuite with BeforeAndAfterAll {
  private var preprocessed: RDD[Structs.Trace_Vector] = _
  private var spark: SparkSession = _
  private var results:List[(String,Int,Int)] =_
  private var log:Structs.Log=_

  override def beforeAll(): Unit = {
    val filename = "input/outliers_30_activities_3k_0.1.xes"
    val results_file = "input/results_30_activities_3k_0.1_description"
    results=Results.read_with_description(results_file)
    val dims = 10
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    log = Utils.Utils.read_xes(filename)
    val traces=Utils.Utils.convert_to_vector_only_durations(log)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    preprocessed =normalizedDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    })
  }

  test("Naive method") {
    val k = 100
    val zeta = results.size
    spark.time({
      val rddDistances = NaiveMethod.initializeDistances(preprocessed, k)
      val sortedByOutlyingFactor = OutlierDetection.assignOutlyingFactor(rddDistances, k)
      val outliers: Array[(Long, Double)] = sortedByOutlyingFactor.sortBy(_._2, ascending = false).collect().slice(0, zeta + 1)
      val found=outliers.count(i => results.map(_._2).contains(i._1))
      println(found.toDouble/results.size)
    })
  }


  test("Ball Tree"){
    val k=50
    var ballTree:BallTree=null
    spark.time({
      ballTree=OurMethod.createBallTree(log,Utils.Utils.convert_to_vector_only_durations)

    })
//    val neiBall=ballTree.kNearestNeighbors(0, k).map(_._1)
//    val neiBall2=ballTree.kNearestNeighbors(1, k).map(_._1)
    val rddDistances = NaiveMethod.initializeDistances(preprocessed, k).collect().toList
    val l:ListBuffer[Double]=new ListBuffer[Double]()
    for(i <- preprocessed.collect().indices) {
      val neiBall=ballTree.kNearestNeighbors(i, k).map(_._1)
      val neighRSquare=rddDistances(i).distances.map(_.id2)
      val found = neiBall.count(i=>neighRSquare.contains(i))
      l+=(found.toDouble/k)
      println(found.toDouble/k)
    }
    print("hi")
  }

}
