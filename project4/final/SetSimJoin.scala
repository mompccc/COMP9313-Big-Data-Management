package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.collection.mutable.Set

object SetSimJoin {
  var hash:Set[(Int, Int)] = Set()
  
  def conbine_frequency(key:Int, line:Array[Int], thres:Double, fre:Map[Int, Int]): Set[(Int, Array[Int])] = {
    var result: Set[(Int, Array[Int])] = Set()

    val whole_line = key +: line
    val sorted_by_fre = line.map(x => fre(x)).sortWith(_ < _) //sort line with frequency and return index
    val prefix = line.length - Math.ceil(line.length.toFloat * thres).toInt + 1 //compute prefix
    val indexList = sorted_by_fre.dropRight(line.length - prefix) //pick front prefix index
    for (fre_index <- indexList) {
      result.add((fre_index, whole_line)) //return in form of (index, line as array)
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
        if(!hash.contains(index_1, index_2)){ //skip compared two lines
          hash.add(index_1, index_2)
          if((words_2.length-1) >= thres*(words_1.length-1)){
            val A = words_1.drop(1).toSet
            val B = words_2.drop(1).toSet
            val total = A ++ B
            val less = A.intersect(B)
            val temp1 = less.size.toDouble/total.size.toDouble
            if(temp1 >= thres){
              if(index_1 < index_2){ //always return index1 < index2
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
    val data = sc.textFile(input).map(x => x.split(" ")).map(x => (x(0).toInt, x.drop(1).map(v => v.toInt))) //map into form of (Int, Array(Int))
    val sort_rdd = data.flatMap(line => line._2.map(x => (x, 1)))
    .reduceByKey(_ + _).sortBy(f => (f._2, f._1)) //compute frequency
    .map(x => x._1.toInt).zipWithIndex() //add index for each token
    .map(f => (f._1, f._2.toInt))
    .collect().toMap //change it to map
    val frequency = sc.broadcast(sort_rdd)

    val Stage2Mapper = data.flatMap(x => conbine_frequency(x._1, x._2, threshold, frequency.value)) //preprogress for each line
    .groupByKey()
    .flatMap(x => reduce(x._2, threshold)) //main function
    .groupBy(f => (f._1, f._2, f._3)).keys //distinct for RDD
    .sortBy(f => (f._1, f._2), true)
    .map(x => "("+x._1.toString()+","+x._2.toString()+")" + "\t" + x._3.toString())
    .saveAsTextFile(output)
  }
}