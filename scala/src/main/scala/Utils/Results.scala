package Utils
import scala.io.Source
object Results {

  def read(filename:String):List[(Int,Int)]={
    Source.fromFile(filename).getLines().map(line=>{
      val x=line.split(",")
      (x(0).toInt,x(1).toInt)
    }).toList
  }

  def read_with_description(filename:String):List[(String,Int,Int)]={
    Source.fromFile(filename).getLines().map(line=>{
      val x=line.split(",")
      (x(0),x(1).toInt,x(2).toInt)
    }).toList
  }

  def read_synthetic(filename:String):List[(String,Int,Int)]={
    val file = Source.fromFile(filename)
    val lines= file.getLines().toList
    file.close()
    lines.map(x=>{
      val spl = x.split(",")
      ("",spl(0).toInt,spl(1).toInt)
    })


  }

}
