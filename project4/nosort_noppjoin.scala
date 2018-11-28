package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.commons.math3.analysis.function.Ceil
import java.util.Arrays
import org.apache.spark.rdd.RDD
import Array._
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._

object SetSimJoin extends Serializable {
  var PairSet: Set[(Int, Int)] = Set()

  def printArray(arr: Array[Int]) {

    for (i <- arr)
      print(i + " ")
    println()
  }
  def printRDDOfPair(arr: RDD[(Int, Int)]) {
    for ((j, k) <- arr)
      println(j + ": " + k)
    println()
  }
  def printArrayOfArray(arr: Array[Array[Int]]) {
    for (i <- arr)
      for (j <- i)
        print(j + " ")
    println()
    println()
  }
  def printRDDOfArrayPair(arr: RDD[Array[(String, Int)]]) {
    for (i <- arr)
      for ((j, k) <- i)
        println(j + ": " + k)
    //println()
    println()
  }

  def printRDDIntArrayInt(array: RDD[(Int, Array[Int])]) {
    for ((k, v) <- array) {
      printf("==key: %s, ", k)
      for (value <- v)
        print(value + " ")
      println()
    }
  }

  def printRDDIntArrayIntPrefix(array: RDD[(Int, Array[Int])], t: Double) {
    for ((k, v) <- array) {
      printf("==key: %s, ", k)
      for (value <- v)
        print(value + " ")
      println("|" + numOfPrefixToken(v.size, t))
    }
  }
  def numOfPrefixToken(length: Int, t: Double): Int = {
    return length - math.ceil(t * length).toInt + 1
  }

  def preappendIndexToArray(index: Int, arr: Array[Int]): Array[Int] = return index +: arr

  //O(N^2) complexity Only computeif record1 index < record2 index, should remove half redundancy
  //  def join(listRecordArray: List[Array[Int]], threshold: Double): Set[(Int, Int, Double)] = {
  //    var result: Set[(Int, Int, Double)] = Set()
  //    val size = listRecordArray.length - 1
  //    for (i <- 0 to size) {
  //      val record1 = listRecordArray(i)
  //      for (j <- i to size) {
  //        val record2 = listRecordArray(j)
  //        val record1_index = record1(0)
  //        val record2_index = record2(0)
  //        if (record1_index < record2_index) {
  //          val record1_value = record1.drop(1).toSet
  //          val record2_value = record2.drop(1).toSet
  //          val intersection = record1_value.intersect(record2_value)
  //          val union = record1_value ++ record2_value
  //          val similarity = intersection.size.toDouble / union.size.toDouble
  //
  //          if (similarity >= threshold) {
  //            result.add((record1_index, record2_index, similarity))
  //          }
  //        }
  //      }
  //    }
  //    return result
  //  }

  def findIndex(value: Array[Int], threshold: Double, broadcastVal: Array[Int]): (Array[Int], Array[Int]) = {
    var indexList: ArrayBuffer[Int] = ArrayBuffer()
    var tempArray = value.to[ArrayBuffer] //: Array[Int] = Array()
    val prefixLength = numOfPrefixToken(value.length, threshold)
    var count = 0
    for (pt <- broadcastVal) {
      if (count < prefixLength) {
        if (value.contains(pt)) {
          indexList += pt
          tempArray -= pt
          count += 1
        }
      }
    }
    val sortedArray = indexList.to[ArrayBuffer] ++ tempArray
    return (indexList.toArray, sortedArray.toArray)
  }
  //Without Sort:assume data sorted
  def stage2MapFunc(index: Int, value: Array[Int], threshold: Double): Set[(Int, Array[Int])] = {
    var result: Set[(Int, Array[Int])] = Set()
    val newValueToSend = preappendIndexToArray(index, value)
    val lastIndex = numOfPrefixToken(value.length, threshold)
    val indexList = value.slice(0, lastIndex)
    for (i <- indexList) {
      result.add((i, newValueToSend))
    }
    return result
  }
  //With Sort:Sort prefix only
  def stage2MapFuncSort(index: Int, value: Array[Int], threshold: Double, broadcastVal: Array[Int]): Set[(Int, Array[Int])] = {
    var result: Set[(Int, Array[Int])] = Set()

    val (indexList, sortedValue) = findIndex(value, threshold, broadcastVal)
    val newValueToSend = preappendIndexToArray(index, sortedValue)
    for (i <- indexList) {
      result.add((i, newValueToSend))
    }
    return result
  }

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val threshold = args(2).toDouble

    //create spark configuration
    val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
    val sc = new SparkContext(conf)

    //read input & split lines
    val inputFile = sc.textFile(inputPath)

    val RDD = inputFile.flatMap(x => x.split("[\n]+")).map(x => x.split(" ")).map(x => (x(0).toInt -> x.drop(1).map(v => v.toInt))) //have index in value
    /*
    //Stage1========================Below line to sort
    val Stage1MapperRDD = RDD.map(line => line._2.map(word => (word, 1)))
    val Stage1ReducerRDD = Stage1MapperRDD.flatMap(list => list).reduceByKey(_ + _) //.map(y => println(y._1 + " " + y._2))
		//    println("=====Frequency List=====")
    //    printRDDOfPair(Stage1ReducerRDD)

    //OrderRDD TO Array:
    val orderArrayToShare = Stage1ReducerRDD.sortBy(_._1).sortBy(_._2).map(x => x._1.toInt).collect().toArray //.foreach(println(_))
    val broadcastVar = sc.broadcast(orderArrayToShare) //cannot broadcast RDD, need to be array/list
		//    println("=====Stage 1 Result:Sorted Array=====")
    //    printRDDIntArrayIntPrefix(sortedRDD, threshold)

    //Stage2 with Sort prefix ========================
    //    val Stage2Mapper = RDD.flatMap(tuple => stage2MapFuncSort(tuple._1, tuple._2, threshold, broadcastVar.value)) //.reduceByKey()
*/
    //Stage2 Assume data sorted ========================
    val Stage2Mapper = RDD.flatMap(tuple => stage2MapFunc(tuple._1, tuple._2, threshold))
    //    println("=====Stage 2 Mapper=====")
    //    printRDDIntArrayInt(Stage2Mapper)

    val Stage2Reducer = Stage2Mapper.aggregateByKey(ListBuffer.empty[Array[Int]])( //Array[(Int, List[Array[Int]])]
      (listBufferArray, array) => { listBufferArray += array; listBufferArray },
      (listBufferArray1, listBufferArray2) => { listBufferArray1.appendAll(listBufferArray2); listBufferArray1 })
      .mapValues(_.toList)
    println("=====Stage 2 Reducer=====")

    //    for ((recordID, recordValue) <- Stage2Reducer) {
    //      print(recordID + ": ")
    //      for (eachRecord <- recordValue) {
    //        for (eachInt <- eachRecord) {
    //          print(eachInt + " ")
    //        }
    //        print("==")
    //      }
    //      println()
    //    }

    //CORRECT ABOVE
    println("=====Stage 2 Step 2 Reducer=====")

    //        val Stage2Reducer_Step2 = Stage2Reducer.flatMap(prevIndexRecord => join(prevIndexRecord._2, threshold))
    val Stage2Reducer_Step2 = Stage2Reducer.flatMap(prevIndexRecord => join(prevIndexRecord._1, prevIndexRecord._2, threshold))
      .groupBy(v => (v._1, v._2, v._3)).keys
      .sortBy(index => (index._1, index._2, true))
      .map(x => "(" + x._1 + "," + x._2 + ")" + "\t" + x._3)
      .saveAsTextFile(outputPath)
    //      .foreach(println)
    //
    println("=====PairSet=====")
    //    for ((i, j) <- PairSet) {
    //      println(i + " " + j)
    //    }
  }

  def join(index: Int, listRecordArray: List[Array[Int]], threshold: Double): Set[(Int, Int, Double)] = {
    //println("====================ENTER KEY = " + index + "==============================")

    var result: Set[(Int, Int, Double)] = Set()
    val size = listRecordArray.length - 1
    for (i <- 0 to size) {
      val record1 = listRecordArray(i)
      val record1_index = record1(0)
      val record1_value = record1.drop(1)
      val record1_value_set = record1_value.toSet
      //ppjoin here
      val record1_prefix = record1_value.indexWhere { case (x) => x == index } + 1
      val record1_left = record1_value.slice(0, record1_prefix).toArray

      for (j <- i to size) {
        breakable {
          val record2 = listRecordArray(j)
          val record2_index = record2(0)

          if (record1_index >= record2_index) break //remove half comparison
          if (record2.length < record1.length * threshold) break //size filtering
          if (PairSet.contains((record1_index, record2_index))) break()
          PairSet.add((record1_index, record2_index))

          val record2_value = record2.drop(1) //.toArray
/*
          //ppjoin here
          val record2_prefix = record2_value.indexWhere { case (x) => x == index } + 1
          val record2_left = record2_value.slice(0, record2_prefix).toArray
          val AAA = record1_left.intersect(record2_left)
          val alpha = math.ceil((record1_value.length + record2_value.length) * threshold / (threshold + 1)).toInt
          val ubound = 1 + math.min(record1_value.length - record1_prefix, record2_value.length - record2_prefix);
          //println("AAA.size= " + AAA.size + " ubound= " + ubound + " alpha =" + alpha)
          if (AAA.size + ubound < alpha) break
*/
          //main comparison here
          val record2_value_set = record2_value.toSet
          val intersection = record1_value_set.intersect(record2_value_set)
          val union = record1_value_set ++ record2_value_set
          val similarity = intersection.size.toDouble / union.size.toDouble

          if (similarity >= threshold) {
            result.add((record1_index, record2_index, similarity))
          }

        }
      }
    }
    return result
  }

}
