import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import oultierDetectionAlgorithms.LofElki

class LOFElkiTest extends FunSuite {

  test("test LoOF from package ELKI") {
    val filename = "input/outliers_30_activities_3k_0.1.xes"
    val results_file = "input/results_30_activities_3k_0.1_description"
    val k=5
    val results = Results.read_with_description(results_file)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val log = Utils.Utils.read_xes(filename)
    spark.time({
      val lof=new LofElki()
      val outliers = lof.assignScore(log, k)
      for(i<-results.indices){
        val r = results(i)
        println(outliers.filter(_._1==r._2).head)
      }
//      outliers.foreach(x=>{
//        println(results.filter(_._2==x._1).head)
//      })

      val found = outliers.count(i => results.map(_._2).contains(i._1))
      println(found.toDouble / results.size)
    })
  }




}
