import org.apache.spark.rdd.RDD

object OutlierDetection {


  def assignOutlyingFactor(distances:RDD[DistancesFromTrace],k:Int):RDD[(Long,Double)]={
    var maxSize=k
    if(k>distances.first().distances.length){
      maxSize=distances.first().distances.length
    }
    distances.map(x=>{
      val element:Double=x.distances.slice(0, maxSize + 1).map(_.distance).sum
      (x.id,element)
    })

  }

  def outlierBasedOnDeviation(scores:RDD[(Long,Double)],n:Int):RDD[(Long,Double)]={
    val mean=scores.map(_._2).mean()
    val std=scores.map(_._2).stdev()
    scores.filter(_._2>mean+n*std)
  }



}
