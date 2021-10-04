package experiments.BPIAnalysis

import Utils.Preprocess
import breeze.linalg.{DenseMatrix, inv}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}
import oultierDetectionAlgorithms.{OurMethod, Structs}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object BPIAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")

    val filename = "input/bpi_2011.xes"
    val log = Utils.Utils.read_xes(filename)
    var exp = new ListBuffer[String]()
    val ks = List(5, 10, 15, 25, 50, 75, 100,150, 200, 250)
//    val ks = List(5)
    val traces = Utils.Utils.convert_to_vector_only_durationsMean(log)
    val preparedForRdd = traces.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    val vector_size = traces.first().elements.length
    val Row(coeff1: Matrix) = Correlation.corr(normalizedDF, "scaledFeatures").head
    val invCovariance: DenseMatrix[Double] = inv(new breeze.linalg.DenseMatrix(vector_size, vector_size, coeff1.toArray))
    val preprocessed = normalizedDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    })
    val distances = OurMethod.initializeDistancesMahalanobis(preprocessed,300,invCovariance).collect()
    //      val distances = OurMethod.initializeDistances(preprocessed,k,Utils.Utils.distance)
    for (k<-ks){
      println(k)
//      val last = distances.map(x=>(x.id,x.distances(k).distance)).sortBy(_._2).map(_._2).collect()
      val last = distances.map(x=>x.distances(k).distance).sortWith((x,y)=>x<y)
      exp += k.toString+","+ last.mkString(",") + "\n"
    }
    val output = "output/kr_test_bpi2011_mahalanobis"
    val file = new File(output)
    val bw = new BufferedWriter(new FileWriter(file))
    exp.toList.foreach(line => {
      bw.write(line)
    })
    bw.close()




  }

}
