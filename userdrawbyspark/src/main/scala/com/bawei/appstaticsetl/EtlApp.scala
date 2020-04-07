package com.bawei.appstaticsetl

import java.text.SimpleDateFormat

import com.bawei.util.JudgeIp
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object EtlApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("etlApp")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.textFile("D:\\yarnData\\appstatics")

    val maped: RDD[String] = text.map(line => {
      val arr = line.split(",")
      var newline = ""
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      try {
        if (arr.length == 5) {
          if (JudgeIp.ipCheck(arr(0))) {
            sdf.parse(arr(3))
            newline = line
          }
        }
      } catch {
        case _ =>
      }

      newline
    })

    val filtered: RDD[String] = maped.filter(line => {
      if (StringUtils.isEmpty(line)) {
        false
      } else {
        true
      }
    })

    filtered.saveAsTextFile("D:\\yarnData\\appstaticsout")
  }

}
