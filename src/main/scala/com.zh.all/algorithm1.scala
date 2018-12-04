package com.zh.all


import org.apache.spark.{SparkConf, SparkContext}


object algorithm1{
  def main(args: Array[String]): Unit = {

    val host="192.168.199.102"
    var conf=new SparkConf().setMaster(host).setAppName("algorithm1").set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    var sc =new SparkContext(conf)


    //            val arr=Seq(34,24,56,78,10,29)
    //            var arrRDD=sc.parallelize(arr)
    //            val sum =arrRDD.reduce(_+_)
    //            println(sum)
    val hashM=Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F",
      "blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "0000",
      "blue" -> "45",
      "yellow" -> "#FFFF00",
      "red" -> "#FF0000")

    //            val mapRdd=sc.parallelize(hashM)
//    val zpath="file:/home/muller/Documents/scalalab/sparkAngorothm/src/main/resources/80input.txt"
//
//    val rawRdd=sc.textFile("file:/home/muller/Documents/scalalab/sparkAngorothm/src/main/resources/80input.txt")

//    val linerdd=  rawRdd.flatMap(line=>line.split(" "))
//    var sumrDD=linerdd.map(line=>line.split("#").map(key=>key.toDouble).toSeq.reduce(_+_))
//    //            sumrDD.map(arr=>arr.)
//    sumrDD.foreach(line=>{
//      println(line)
//      println("*************")
//    })
    //            linerdd.flatMap( line=>
    //                    {
    //                        val keys= line.split("#")
    //                        val sum =keys.map(key=>key.toDouble).reduce(_+_)
    //                        sum
    //                    }
    //
    //            )
    //            rawRdd.saveAsTextFile("./out.txt")

    var seqMap=Seq(  "red" -> "#FF0000",  "yellow" -> "#FFFF00","azure" -> "#F0FFFF",
      "peru" -> "#CD853F",
      "blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "0000",
      "blue" -> "45",
      "yellow" -> "#FFFF00",
      "red" -> "#FF0000")

    //            rawRdd.map{
    //                line=>
    //                    println("he")
    //                    println(rawRdd)
    //            }
    val  seqRdd=sc.parallelize(seqMap)

    seqRdd.foreach(line =>print(line._2))
    val newRdd=seqRdd.map(line=> (line._1,line._2)).reduceByKey(_+_)

    newRdd.foreach(line=>print(line._1+"   "+line._2+"\n"))
    // seqRdd.map(line=> print(line._2))
    //    (line._1,line._2)) //.reduceByKey(_.mkString(_)).map(lne=>print(lne))
    println("spark running")

    val data=List((1,3),(1,2),(1,4),(2,3))
    val rdd=sc.parallelize(data, 2)

    //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
    def combOp(a:String,b:String):String={
      println("combOp: "+a+"\t"+b)
      a+b
    }
    //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seqOp(a:String,b:Int):String={
      println("SeqOp:"+a+"\t"+b)
      a+b
    }
    rdd.foreach(println)
    //zeroValue:中立值,定义返回value的类型，并参与运算
    //seqOp:用来在同一个partition中合并值
    //combOp:用来在不同partiton中合并值
    val aggregateByKeyRDD=rdd.aggregateByKey("100")(seqOp, combOp)


    Thread.sleep(1)
    aggregateByKeyRDD.foreach(line=>print(line._1+"   "+line._2+"\n"))

  }


}

class algorithm1 {

}
