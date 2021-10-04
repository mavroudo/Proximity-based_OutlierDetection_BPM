package experiments

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.OurMethod

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object Experiment_classicDefinition {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("Temporal trace anomaly detection")
//      .master("local[*]")
//      .getOrCreate()
//    println(s"Starting Spark version ${spark.version}")
//
//    val ks = List(5,10,15,20,30,40,50,75,100)
//    val rs= List(0.1,0.5,0.75,1,1.25,1.5,1.75,2,3,5)
//    val files=List("30_activities_10k_0.01","30_activities_10k_0.05","30_activities_10k_0.1")
//
//    for (dataset<-files){
//      val filename = "input/outliers_"+dataset+".xes"
//      val log = Utils.Utils.read_xes(filename)
//      val results_file = "input/results_"+dataset+"_description"
//      val results=Results.read_with_description(results_file)
//      var exp=new ListBuffer[String]()
//      for(r<-rs){
//        println(dataset,r)
//        val k =50
//        var t1 = System.nanoTime
//        val outliers = OurMethod.findOutliers(log,k,r,Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distanceEuclidean)
//        val found=outliers.count(i => results.map(_._2).contains(i)).toDouble
//        var duration = (System.nanoTime - t1) / 1e9d
//        val precision = found.toDouble / outliers.length
//        val recall = found.toDouble / results.length
//        exp+=dataset+","+k.toString+","+r.toString+","+duration.toString+","+precision.toString + "," + recall.toString + "\n"
//      }
//      val output="output/"+dataset+"_rs"
//      val file = new File(output)
//      val bw = new BufferedWriter(new FileWriter(file))
//      exp.toList.foreach(line=>{
//        bw.write(line)
//      })
//      bw.close()
//      for(k<-ks){
//        println(dataset,k)
//        val r = 0.5
//        var t1 = System.nanoTime
//        val outliers = OurMethod.findOutliers(log,k,r,Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distanceEuclidean)
//        val found=outliers.count(i => results.map(_._2).contains(i)).toDouble
//        var duration = (System.nanoTime - t1) / 1e9d
//        val precision = found.toDouble / outliers.length
//        val recall = found.toDouble / results.length
//        exp+=dataset+","+k.toString+","+r.toString+","+duration.toString+","+precision.toString + "," + recall.toString + "\n"
//      }
//      val output2="output/"+dataset+"_ks"
//      val file2 = new File(output2)
//      val bw2 = new BufferedWriter(new FileWriter(file2))
//      exp.toList.foreach(line=>{
//        bw2.write(line)
//      })
//      bw2.close()
//
//
//    }
//  }
}
