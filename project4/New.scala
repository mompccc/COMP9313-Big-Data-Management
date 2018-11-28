package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.collection.mutable.Set

object New {
  var hash:Set[(Int, Int)] = Set()
  
  def haha(line:String, thres:Double) : Set[(Int, Array[Int])] = {
    var result:Set[(Int, Array[Int])] = Set()
    val temp = line.split(" ").map(_.toInt)
    val s = temp.drop(1)
    val prefix = s.length - Math.ceil(s.length.toFloat * thres).toInt + 1
    for(i <- 0 to prefix-1){
      result.add(s(i), temp)
    }
    return result
  }
  
  def reduce(line:Iterable[Array[Int]], thres:Double) : Set[(Int, Int, Double)] = {
    var result:Set[(Int, Int, Double)] = Set()
    val list = line.toArray
    val len = list.length-1
    for(i <- 0 to len){
      val words_1 = list(i)
      val index_1 = words_1(0)
      for(j <- i+1 to len){
        val words_2 = list(j)
        val index_2 = words_2(0)
        if(!hash.contains(index_1, index_2)){
          hash.add(index_1, index_2)
          if((words_2.length-1) >= thres*(words_1.length-1)){
            val A = words_1.drop(1).toSet
            val B = words_2.drop(1).toSet
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
    return result
  }
  
  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val threshold = args(2).toDouble
    val conf = new SparkConf().setAppName("project4")
    val sc = new SparkContext(conf)
    val data = sc.textFile(input)
    //val sort_rdd = data.flatMap(line => line.split(" ").drop(1))
    //.map(word => (word, 1)).reduceByKey(_+_)
    //.sortBy(f => f._2)
    //.map{case (key, value) => key}
    
    val test1 = data.flatMap(line => haha(line, threshold))
    .groupByKey()
    .flatMap(x => reduce(x._2, threshold))
    .groupBy(f => (f._1, f._2, f._3)).keys
    .sortBy(f => (f._1, f._2), true)
    .map(x => "("+x._1.toString()+","+x._2.toString()+")" + "\t" + x._3.toString())
    .saveAsTextFile(output)
    //test1.foreach(println)
  }
}