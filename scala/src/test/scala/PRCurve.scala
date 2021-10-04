import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.{LOF_core, ODAL, OurMethod, Structs}

class PRCurve extends FunSuite with BeforeAndAfterAll{
  private var spark: SparkSession = _
  private var results:List[(String,Int,Int)] =_
  private var log:Structs.Log=_

  override def beforeAll(): Unit = {
    val filename = "input/outliers_30_activities_10k_0.01.xes"
    val results_file = "input/results_30_activities_10k_0.01_description"
    results=Results.read_with_description(results_file)
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    log = Utils.Utils.read_xes(filename)
  }

  test("Precision - Recall for zeta value"){
    val zetaPortion = results.length/10
    val k = 100
    val scores = OurMethod.assignOutlyingFactor(log,k,Utils.Utils.convert_to_vector_only_durationsMean,Utils.Utils.distance,"mahalanobis")
//    val scoreslof = LOF_core.assignOutlierFactor(log,k,Utils.Utils.convert_to_vector_only_durationsMean,Utils.Utils.distance,"mahalanobis")
    for(i <- 1 to 10){
      val found=scores.slice(0,zetaPortion*i).count(i=>results.map(_._2).contains(i._1.toInt))
      println(found,zetaPortion*i)
    }
  }

  test("Precision -Recall for n values"){
    val ns = List(4,3.8,3.6,3.4,3.2,3,2.8,2.6,2.4,2.2,2,1.5,1,0.5,0.2)
    val k = 1
    for (n <-ns){
//      val outliers2 = OurMethod.findOutliersStatistical(log, k, n, Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distance,"mahalanobis")
      val outliers2 = ODAL.find_outliers_admition(log,k,n.toFloat)
      val found2 = outliers2.count(i => results.map(_._2).contains(i)).toDouble
      println(found2,outliers2.length)
    }
  }

  test("Precision - Recall for distance based"){
    val r =4
    val k =50
    val outliers2 = OurMethod.findOutliers(log, k, r, Utils.Utils.convert_to_vector_only_durationsMean,Utils.Utils.distance,"mahalanobis")
    val found2 = outliers2.count(i => results.map(_._2).contains(i)).toDouble
    println(found2,outliers2.length)
  }
}
