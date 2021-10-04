import Utils.{Mahalanobis, Preprocess, Results}
import breeze.linalg.{DenseMatrix, inv}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.{OurMethod, Structs}

class OurMethodTest extends FunSuite with BeforeAndAfterAll{
  private var spark: SparkSession = _
  private var results:List[(String,Int,Int)] =_
  private var log:Structs.Log=_

  override def beforeAll(): Unit = {
    val filename = "input/outliers_30_activities_10k_0.1.xes"
    val results_file = "input/results_30_activities_10k_0.1_description"
    results=Results.read_with_description(results_file)
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    log = Utils.Utils.read_xes(filename)
  }

  test("Find Outliers"){
    val k=10
    val r =0.5
    val outliers = OurMethod.assignOutlyingFactor(log,k,Utils.Utils.convert_to_vector_both_duration_repetitions,Utils.Utils.distance,"rmse")

    Utils.Utils.mean(outliers.map(_._2))
//    val found=outliers.count(i => results.map(_._2).contains(i.toInt))
//    println(found.toDouble/results.size)
    println("hi")
  }

  test("Mahalanobis Distance"){
    val k=10
    val scores = OurMethod.assignOutlyingFactor(log,k,Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distance,"mahalanobis")
    val found=scores.slice(0,results.size+1).count(i=>results.map(_._2).contains(i._1.toInt))
    println(found.toDouble/results.size)
  }

  test("Ball Tree outliers"){
    val k=50
    val ballTree = OurMethod.createBallTree(log,Utils.Utils.convert_to_vector_only_durations)
    val outliers = OurMethod.assignOutlyingFactorWithBallTree(log,ballTree, k).slice(0, results.size)
    val found = outliers.count(i => results.map(_._2).contains(i._1)).toDouble / results.size
    println(found.toDouble/results.size)
  }

  test("Row by row mahalanobis"){
    val traces = Utils.Utils.convert_to_vector_only_durations(log)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    val vector_size = traces.first().elements.length
    val Row(coeff1: Matrix) = Correlation.corr(normalizedDF, "scaledFeatures").head
    val invCovariance: DenseMatrix[Double] = inv(new breeze.linalg.DenseMatrix(vector_size, vector_size, coeff1.toArray))
    val preprocessed = normalizedDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    })

    val mah = new Mahalanobis(invCovariance)
    val tr = traces.collect()
    val init = OurMethod.initializeDistancesMahalanobis(traces,10,invCovariance)
    val scores = init.map(x => {
      val element: Double = x.distances.slice(0, 10 + 1).map(_.distance).sum
      (x.id, element)
    })
    val found = scores.collect().sortWith((x,y)=>x._2>y._2).slice(0,1000).map(_._1).count(i=>results.map(_._2).contains(i))

    println(mah.distance(tr(0),tr(1)))
  }

  test("traditional with normalized"){
    val filename = "input/bpi_2015.xes"
    log = Utils.Utils.read_xes(filename)
    val k = 5
    val r = 0.3

    val outliers2=OurMethod.assignOutlyingFactor(log, k,converter = Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distance,"rmse")
    outliers2.slice(0,10).foreach(println)
    println("Now the second")
    val outliers=OurMethod.findOutliers(log,k,r,converter = Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distance,"rmse")
    outliers.foreach(println)
    println(outliers.length)
  }


}
