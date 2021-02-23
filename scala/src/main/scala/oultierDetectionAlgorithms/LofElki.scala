package oultierDetectionAlgorithms

import Utils.Preprocess
import de.lmu.ifi.dbs.elki.database.StaticArrayDatabase
import de.lmu.ifi.dbs.elki.datasource.{ArrayAdapterDatabaseConnection, DatabaseConnection}
import oultierDetectionAlgorithms.Structs.Trace_Vector
import de.lmu.ifi.dbs.elki.algorithm.outlier.lof.LOF
import de.lmu.ifi.dbs.elki.data.NumberVector
import de.lmu.ifi.dbs.elki.database.ids.{DBIDIter, DBIDUtil}
import de.lmu.ifi.dbs.elki.database.relation.DoubleRelation
import de.lmu.ifi.dbs.elki.distance.distancefunction.minkowski.{EuclideanDistanceFunction, SquaredEuclideanDistanceFunction}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class LofElki {

  def assignScore(log:Structs.Log,k:Int):Array[(Int,Double)]={
    val data = this.converter(log)
    val dbc: DatabaseConnection = new ArrayAdapterDatabaseConnection(data)
    val db = new StaticArrayDatabase(dbc, null)
    db.initialize()
    val lof = new LOF[NumberVector](k,EuclideanDistanceFunction.STATIC)
    val results = lof.run(db)
    val scores: DoubleRelation = results.getScores
    val iter: DBIDIter = scores.iterDBIDs();
    val scoresArray: Array[(Int, Double)] = new Array(log.traces.count().toInt)
    var counter=0
    while (iter.valid()) {
      scoresArray(counter) = (DBIDUtil.toString(iter).toInt, scores.doubleValue(iter))
      counter+=1
      iter.advance()
    }
    scoresArray.sortWith((x, y) => x._2 > y._2)


  }

  def converter(log: Structs.Log): Array[Array[Double]] = {
    val spark = SparkSession.builder().getOrCreate()
    val transformed: RDD[Trace_Vector] = Utils.Utils.convert_to_vector_only_durations(log)
    val preparedForRdd = transformed.map(x => Tuple2.apply(x.id, Vectors.dense(x.elements)))
    val df = spark.createDataFrame(preparedForRdd).toDF("id", "features")
    val normalizedDF = Preprocess.normalize(df)
    val traces=normalizedDF.rdd.map(row=>{
      Structs.Trace_Vector(row.getAs[Long]("id"), row.getAs[DenseVector]("scaledFeatures").values)
    }).collect()
    val a = Array.ofDim[Double](traces.length, traces.head.elements.length)
    for (t_index <- traces.indices) {
      val trace = traces(t_index).elements
      for (v_index <- traces.head.elements.indices) {
        a(t_index)(v_index) = trace(v_index)
      }
    }
    a

  }

}
