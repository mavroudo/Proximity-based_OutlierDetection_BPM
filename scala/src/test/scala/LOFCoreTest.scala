import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.{LOF_core, Structs}

class LOFCoreTest extends FunSuite with BeforeAndAfterAll{
  private var spark: SparkSession = _
  private var results:List[(String,Int,Int)] =_
  private var log:Structs.Log=_

  override def beforeAll(): Unit = {
    val filename = "input/outliers_30_activities_10k_0.1.xes"
    val results_file = "input/results_30_activities_10k_0.1_description"
    results = Results.read_with_description(results_file)
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    log = Utils.Utils.read_xes(filename)

  }

  test("Test lof core"){
//    val k=50
//    val scores=LOF_core.assignOutlierFactor(log,k,Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distanceEuclidean)
//    val found=scores.slice(0,results.size+1).count(i=>results.map(_._2).contains(i._1.toInt))
//    println(found.toDouble/results.size)
  }

  test("Mahalanobis distance"){
        val k=50
        val scores=LOF_core.assignOutlierFactor(log,k,Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distanceEuclidean,"mahalanobis")
        val found=scores.slice(0,results.size+1).count(i=>results.map(_._2).contains(i._1.toInt))
        println(found.toDouble/results.size)
  }

  test("LOF per event"){
    val k=50
    val zeta =1000
    val outliers= LOF_core.findOutlierEvents(log,k,zeta)
    val groundTruth= results.map(x=>(x._2,x._3))
    val found = outliers.count(x=>groundTruth.contains((x._1.trace_index.toInt,x._1.event_pos.toInt)))
    println(found.toDouble/results.size)
  }

}
