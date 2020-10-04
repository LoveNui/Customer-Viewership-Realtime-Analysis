package com.analytics.retail
import org.apache.spark.sql.functions._

object UDFs {
  def emailUDF = udf {
    email: String => email.replaceAll("(?<=@)[^.]+(?=\\.)", "*****")

  }
  def cellUDF = udf {
    cell:String => cell.replaceAll("[^0-9]","")
  }
  def ageUDF = udf {
    age:String => val md = java.security.MessageDigest.getInstance("SHA-1")
      val ha = new sun.misc.BASE64Encoder().encode(md.digest(age.getBytes))
      ha
  }

  // 140-333-4444
  // 1403334444, 5
  // 14033XXXXX
  //substring will start from 0

  def posUDF = udf {
    (cell: String, pos :Int) => cell.substring(0, cell.length - pos).concat("X" * pos)
  }
}
