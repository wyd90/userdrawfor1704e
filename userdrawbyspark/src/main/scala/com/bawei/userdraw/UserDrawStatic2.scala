package com.bawei.userdraw

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object UserDrawStatic2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("userdraw")
    val sc = new SparkContext(conf)

    //读取用户每天的日志信息
    val text: RDD[String] = sc.textFile("C:\\yarnData\\userDraw\\input")


    //读取标签库信息
    val tagText: RDD[String] = sc.textFile("C:\\yarnData\\userDraw\\appData")
    //将标签库信息转换为元组
    val tagsRdd: RDD[(String, String, Double, Double, Double, Double, Double, Double, Double)] = tagText.map(line => {
      val arr: Array[String] = line.split("[|]")
      (arr(0), arr(1), arr(2).toDouble, arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble, arr(8).toDouble)
    })
    //将标签库信息收回driver端
    val rules: Array[(String, String, Double, Double, Double, Double, Double, Double, Double)] = tagsRdd.collect()

    //将标签库信息在driver端转换为map结构，以appId做key
    val ruleMap = new mutable.HashMap[String,(String, String, Double, Double, Double, Double, Double, Double, Double)]()
    rules.foreach(x => {
      ruleMap.put(x._1,(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9))
    })
    //将标签库map广播到executor中
    val ruleMapBroad: Broadcast[mutable.HashMap[String, (String, String, Double, Double, Double, Double, Double, Double, Double)]] = sc.broadcast(ruleMap)


    //清洗用户日志信息，提取有用的部分
    val maped: RDD[(String, String, Long)] = text.map(line => {
      val arr: Array[String] = line.split("[|]")
      var mdn = "NULL"
      var appId = 0
      var times = -1L;
      try {
        mdn = arr(0)
        appId = arr(15).toInt
        times = arr(12).toLong
      } catch {
        case _: Exception =>
      }
      (mdn, ""+appId, times)
    })

    //过滤垃圾数据
    val filtered = maped.filter(x => {
      if (StringUtils.isEmpty(x._1) || "NULL".equals(x._1) || "0".equals(x._2) || (-1L).equals(x._3)) {
        false
      } else {
        true
      }
    })

    println("用户数据数="+filtered.map(x => x._1).distinct().count())
    //以 用户 + appId 分组，计算这个用户这个appId使用时长
    val mdnAndAppIdUseTimes: RDD[((String, String), Long)] = filtered.map(x => ((x._1,x._2),x._3)).reduceByKey(_ + _)

    //把用户日志信息变成 pair元组
    val logs: RDD[(String,(Double, Double, Double, Double, Double, Double, Double, String, Long,String))] = mdnAndAppIdUseTimes.map(x => {
      //mdn     男概率，女概率，age1概率，age2概率，age3概率，age4概率，age5概率，appId，这款app使用的时长，标志位：表明这条信息是用户日志信息
      (x._1._1,(-1d, -1d, -1d, -1d, -1d, -1d, -1d, x._1._2, x._2,"logmsg"))
    })
    //从hbase中把用户之前的用户画像调出来
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quor" +
      "um","node4:2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"ns6:t_draw")

    val scan = new Scan
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN,scanToString)

    val userDrawData: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf
      , classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable]
      , classOf[Result])

    println("hbases数据条数="+userDrawData.count())

    //将hbase中读取到的信息做变换
    val fromhbase: RDD[(String,(Double, Double, Double, Double, Double, Double,Double,String,Long,String))] = userDrawData.map(x => {
      //mdn
      val rowKey = Bytes.toString(x._1.get(), x._1.getOffset, x._1.getLength)
      val maleByte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("male"))
      val femaleByte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("female"))
      val age1Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age1"))
      val age2Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age2"))
      val age3Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age3"))
      val age4Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age4"))
      val age5Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age5"))

      (rowKey
        ,(Bytes.toDouble(maleByte)
        , Bytes.toDouble(femaleByte)
        , Bytes.toDouble(age1Byte)
        , Bytes.toDouble(age2Byte)
        , Bytes.toDouble(age3Byte)
        , Bytes.toDouble(age4Byte)
        , Bytes.toDouble(age5Byte)
        ,"NULL"    //appId
        ,-1L     //times
        ,"hbasemsg"))  //标志位：表示这条信息来自从hbase读取
    })

    //将用户日志信息和从hbase中取出的用户画像做并
    val datas: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long,String))] = logs.union(fromhbase)



    val partitioned = datas.partitionBy(new MyPartitioner(4))

    println("分区后的条数="+partitioned.count())

    partitioned.mapPartitionsWithIndex((x,it) => {
      println(s"区号${x}条数是="+it.toList.size)
      it
    }).collect()



    implicit val userOrdering = new Ordering[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long,String))] {
      override def compare(x: (String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String)), y: (String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))): Int = {
        //先把同mdn的数据排到一起
        if(x._1.compareTo(y._1) == 0) {
          //把hbase的信息排第一个
          if(x._2._10.compareTo(y._2._10) == 0) {
            x._2._8.compareTo(y._2._8)
          } else {
            if("hbasemsg".equals(y._2._10)) {
              1
            } else {
              -1
            }
          }
        } else {
          x._1.compareTo(y._1)
        }
      }
    }
//
//    //将分好区的数据进行排序
    val sorted: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))] = partitioned.sortBy(x => x)

    println("排好序后条数是="+sorted.count())

    sorted.mapPartitionsWithIndex((x,it) => {
      println(s"排序后，区号${x}数据条数是="+it.toList.size)
      it
    }).collect()

    //16807
    println("排序后的总用户数="+sorted.map(_._1).distinct().count())

    sorted.map(_._1).distinct().mapPartitionsWithIndex((x,it) => {
      println(s"排序后，区号${x}用户条数是="+it.toList.size)
      it
    }).collect()





//
//  计算
    sorted.mapPartitionsWithIndex((x,it) => {
      val resList: ListBuffer[UserDraw] = new collection.mutable.ListBuffer[UserDraw]

      val distinct = it.toList.map(x => x._1).distinct

      val userMap: Map[String, List[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))]] = it.toList.groupBy(_._1)
      val rule = ruleMapBroad.value


      for((k,v) <- userMap) {
        val userDraw = new UserDraw(k)
        for(i <- 0 to v.length - 1) {

          if(i == 0) {
            //判断第一条数据是不是hbased的数据
            if("hbasemsg".equals(v(i)._2._10)) {
              //如果是,取出
              userDraw.male = v(i)._2._1
              userDraw.female = v(i)._2._2
              userDraw.age1 = v(i)._2._3
              userDraw.age2 = v(i)._2._4
              userDraw.age3 = v(i)._2._5
              userDraw.age4 = v(i)._2._6
              userDraw.age5 = v(i)._2._7
            } else {
              //如果不是，进行计算
              val tagOp = rule.get(v(i)._2._8)
              tagOp match{
                case Some(a) => {
                  userDraw.protraitSex(a._3,a._4,v(i)._2._9)
                  userDraw.protraitAge(a._5,a._6,a._7,a._8,a._9,v(i)._2._9)
                }
                case None =>
              }

            }
          } else {
            //不是第一条数据
            val tagOp = rule.get(v(i)._2._8)
            tagOp match{
              case Some(a) => {
                userDraw.protraitSex(a._3,a._4,v(i)._2._9)
                userDraw.protraitAge(a._5,a._6,a._7,a._8,a._9,v(i)._2._9)
              }
              case None =>
            }
          }
        }
        resList.append(userDraw)
      }

      println(s"计算后，区号${x}用户条数是="+distinct.size)
      resList.toIterator
    }).collect()

    //res.take(100).foreach(x => println(x))
    //println(res.map(x => x.mdn).distinct().count())
    //TODO save hbase ...
    sc.stop()

  }

}

//自定义分区器，按照mdn分器
class MyPartitioner2(partitionNum: Int) extends Partitioner {

  override def numPartitions: Int = partitionNum

  override def getPartition(key: Any): Int = {
    val mdn = key.asInstanceOf[String]
    (mdn.hashCode & Integer.MAX_VALUE) % partitionNum
  }
}
