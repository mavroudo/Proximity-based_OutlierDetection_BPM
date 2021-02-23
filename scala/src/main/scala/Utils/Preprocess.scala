package Utils

import org.apache.spark.ml.feature.{PCA, StandardScaler}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import oultierDetectionAlgorithms.Structs

object Preprocess {


  def preprocess(log: Structs.Log, dimensions: Int, converter: Structs.Log => RDD[Structs.Trace_Vector]): RDD[Structs.Trace_Vector] = {
    val spark = SparkSession.builder().getOrCreate()
    val transformed = converter(log) //create vectors
    val preparedForRdd = transformed.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements))) //make it dataframe
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = this.normalize(df) //normalize data
    val reduceDimensionalityDF = this.reduceDimensionalityPCA(normalizedDF, dimensions) //apply pca
    reduceDimensionalityDF.rdd.map(row => {
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("pcaFeatures").values)
    })
  }

  def normalize(log: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)
      .fit(log)
    scaler.transform(log)
  }

  def reduceDimensionalityPCA(log: DataFrame, k: Int): DataFrame = {
    val pca = new PCA()
      .setInputCol("scaledFeatures")
      .setOutputCol("pcaFeatures")
      .setK(k)
      .fit(log)
    pca.transform(log)
  }


}
