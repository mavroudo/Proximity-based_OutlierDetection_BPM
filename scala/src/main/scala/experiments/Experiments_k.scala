package experiments

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{LOF_core, LofElki, ODAL, OurMethod, Structs}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object Experiments_k {

//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("Temporal trace anomaly detection")
//      .master("local[*]")
//      .getOrCreate()
//    println(s"Starting Spark version ${spark.version}")
//
//    val ks=List(1,3,10,15,20,30,40,50)
//    val files=List("30_activities_10k_0.01","30_activities_10k_0.05","30_activities_10k_0.1")
//    val converter:Structs.Log=>RDD[Structs.Trace_Vector]=Utils.Utils.convert_to_vector_only_durations
//    for (dataset<-files){
//      val output="output/"+dataset
//      val filename = "input/outliers_"+dataset+".xes"
//      val log = Utils.Utils.read_xes(filename)
//      val results_file = "input/results_"+dataset+"_description"
//      val results=Results.read_with_description(results_file)
//      var exp=new ListBuffer[String]()
//      for(k<-ks){
//        println(dataset,k)
//        //ours with r =0.5
//        val r=0.5
//        val t0 = System.nanoTime
//        val outliers0=OurMethod.findOutliers(log,k,r,converter,Utils.Utils.distanceEuclidean)
//        val found0=outliers0.count(i => results.map(_._2).contains(i)).toDouble/results.size
//        val duration0 = (System.nanoTime - t0) / 1e9d
//        exp+="Classic Definition,"+dataset+","+k.toString+","+duration0.toString+","+found0.toString+"\n"
//        //Ours
//        val t1 = System.nanoTime
//        val outliers=OurMethod.assignOutlyingFactor(log,k,converter,Utils.Utils.distanceEuclidean).slice(0, results.size)
//        val found=outliers.count(i => results.map(_._2).contains(i._1)).toDouble/results.size
//        val duration = (System.nanoTime - t1) / 1e9d
//        exp+="Our Method,"+dataset+","+k.toString+","+duration.toString+","+found.toString+"\n"
//        //LOF
//        val t2 = System.nanoTime
//        val outliers2 = LOF_core.assignOutlierFactor(log,k,converter,Utils.Utils.distanceEuclidean).slice(0, results.size)
//        val found2=outliers2.count(i => results.map(_._2).contains(i._1)).toDouble/results.size
//        val duration2 = (System.nanoTime - t2) / 1e9d
//        exp+="LOF,"+dataset+","+k.toString+","+duration2.toString+","+found2.toString+"\n"
//        //ODAL
//        val t3 = System.nanoTime
//        val outliers3 = ODAL.find_outliers(log, k, 3)
//        val found3 = outliers3.count(i => results.map(_._2).contains(i)).toDouble/results.size
//        val duration3 = (System.nanoTime - t3) / 1e9d
//        exp+="ODAL,"+dataset+","+k.toString+","+duration3.toString+","+found3.toString+"\n"
//      }
//      val file = new File(output)
//      val bw = new BufferedWriter(new FileWriter(file))
//      exp.toList.foreach(line=>{
//        bw.write(line)
//      })
//      bw.close()
//    }
//    val output="output/30_activities_3k_0.01"
//
//    val filename = "input/outliers_30_activities_3k_0.01.xes"
//    val log = Utils.Utils.read_xes(filename)
//    val results_file = "input/results_30_activities_3k_0.01"
//    val results=Results.read(results_file)









//  }


}
