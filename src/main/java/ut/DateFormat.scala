package ut

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

object DateFormat {
  def main(args: Array[String]): Unit = {


    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var date: Date = null

      date = format.parse("1578247200000")
      print(date)


  }

}
