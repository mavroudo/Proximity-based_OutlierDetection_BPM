import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.util.Date

import Utils.Utils
import org.scalatest.FunSuite

class UtilsTest extends  FunSuite{
  test("Calculating Duration"){
    val previousString:String="2011-10-01T01:38:44.546+03:00"
    val nowString:String="2011-10-01T01:38:44.880+03:00"
    val previousDate = Date.from(ZonedDateTime.parse(previousString).toInstant)
    val nowDate = Date.from(ZonedDateTime.parse(nowString).toInstant)
    assert(Utils.duration(previousDate,nowDate)==334)
  }
}
