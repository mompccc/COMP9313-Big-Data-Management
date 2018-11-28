package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.collection.mutable.Set

object Test {
  var hash:Set[(Int, Int)] = Set()
  
  def haha(line:String, thres:Double) : Set[Array[String]] = {
    var result:Set[Array[String]] = Set()
    val s = line.split(" ").drop(1)
    val prefix = s.length - Math.ceil(s.length.toFloat * thres).toInt + 1
    for(i <- 0 to prefix-1){
      result.add(Array(s(i), line))
    }
    return result
  }
  
  def reduce(tt:String, line:String, thres:Double, fre:Array[String]) : Set[(Int, Int, Double)] = {
    var result:Set[(Int, Int, Double)] = Set()
    val list = line.split("-")
    val len = list.length-1
    for(i <- 0 to len){
      for(j <- i+1 to len){
        val words_1 = list(i).split(" ")
        val words_2 = list(j).split(" ")
        val index_1 = words_1(0).toInt
        val index_2 = words_2(0).toInt
        if(!hash.contains(index_1, index_2)){
          hash.add(index_1, index_2)
          if((words_2.length-1) >= thres*(words_1.length-1)){
            val s1 = words_1.drop(1)
            val s2 = words_2.drop(1)
            val l1 = s1.length
            val l2 = s2.length
            val prefix_1 = l1 - Math.ceil(l1.toFloat * thres).toInt + 1
            val prefix_2 = l2 - Math.ceil(l2.toFloat * thres).toInt + 1
            val AAA = s1.dropRight(l1-prefix_1).toSet.intersect(s2.dropRight(l2-prefix_2).toSet).size
            val alpha = Math.ceil((thres/(1.0+thres))*(l1.toFloat + l2.toFloat)).toInt
            val ubound = 1 + Math.min(l1 - (s1.indexOf(tt)+1), l2 - (s2.indexOf(tt)+1))
            if((AAA + ubound) >= alpha){
              var O = AAA
              val index_x = fre.indexOf(s1(prefix_1-1))
              val index_y = fre.indexOf(s2(prefix_2-1))
              if(index_x < index_y){
                val ubound = AAA + l1 - prefix_1
                if(ubound >= alpha){
                  O += (s1.drop(prefix_1).toSet & s2.drop(AAA).toSet).size
                }
              }
              else{
                val ubound = AAA + l2 - prefix_2
                if(ubound >= alpha){
                  O += (s1.drop(AAA).toSet & s2.drop(prefix_2).toSet).size
                }
              }
              if(O >= alpha){
                val A = s1.toSet
                val B = s2.toSet
                val total = A ++ B
                val less = A.intersect(B)
                val temp1 = less.size.toDouble/total.size.toDouble
                if(temp1 >= thres){
                  if(index_1 < index_2){
                    result.add((index_1, index_2, temp1))
                  }
                else{
                  result.add((index_2, index_1, temp1))
                  }
                }
              }
            }
          }
        }
      }
    }
    return result
  }
  
  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val threshold = args(2).toDouble
    val conf = new SparkConf().setAppName("project4")
    val sc = new SparkContext(conf)
    val data = sc.textFile(input)
    val sort_rdd = data.flatMap(line => line.split(" ").drop(1))
    .map(word => (word, 1)).reduceByKey(_+_)
    .sortBy(f => (f._2, f._1))
    .map{case (key, value) => key}
    val frequency = sc.broadcast(sort_rdd.collect())

    val test1 = data.flatMap(line => haha(line, threshold))
    .map(x => (x(0), x(1)))
    .reduceByKey(_+"-"+_)
    .flatMap(x => reduce(x._1, x._2, threshold, frequency.value))
    .groupBy(f => (f._1, f._2, f._3)).keys
    .sortBy(f => (f._1, f._2), true)
    .map(x => "("+x._1.toString()+","+x._2.toString()+")" + "\t" + x._3.toString())
    .saveAsTextFile(output)
    //test1.foreach(println)
  }
}