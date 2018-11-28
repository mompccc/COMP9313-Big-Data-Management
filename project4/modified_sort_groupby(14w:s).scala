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
import scala.collection.mutable
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
  //  def stage2MapFuncSort(index: Int, value: Array[Int], threshold: Double, broadcastVal: Array[Int]): Set[(Int, Array[Int])] = {
  //    var result: Set[(Int, Array[Int])] = Set()
  //
  //    val (indexList, sortedValue) = findIndex(value, threshold, broadcastVal)
  //    val newValueToSend = preappendIndexToArray(index, sortedValue)
  //    for (i <- indexList) {
  //      result.add((i, newValueToSend))
  //    }
  //    return result
  //  }

  //Map[Int, Long]
  def stage2MapFuncSort(index: Int, value: Array[Int], threshold: Double, broadcastVal: Map[Int, Int]): Set[(Int, Array[Int])] = {
    var result: Set[(Int, Array[Int])] = Set()

    //change value to index
    val sortedValue = value.map(x => broadcastVal(x)).sortWith(_ < _)
    //    println("Sorted: " + sortedValue)
    val newValueToSend = index +: sortedValue
    val lastIndex = numOfPrefixToken(value.length, threshold)
    val indexList = sortedValue.slice(0, lastIndex)
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
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)

    //read input & split lines
    val inputFile = sc.textFile(inputPath)

    val RDD = inputFile.flatMap(x => x.split("[\n]+")).map(x => x.split(" ")).map(x => (x(0).toInt -> x.drop(1).map(v => v.toInt))) //have index in value

    //Stage1========================Below line to sort
    val Stage1MapperRDD = RDD.map(line => line._2.map(word => (word, 1)))
    val Stage1ReducerRDD = Stage1MapperRDD.flatMap(list => list).reduceByKey(_ + _) //.map(y => println(y._1 + " " + y._2))
    println("=====Frequency List=====")
    //    printRDDOfPair(Stage1ReducerRDD)

    //OrderRDD TO Map:
    val orderArrayToShare = Stage1ReducerRDD /*sortBy(_._1).*/ .sortBy(_._2).map(x => x._1.toInt).zipWithIndex().collect().toMap.map(x => (x._1 -> x._2.toInt))

    val broadcastVar = sc.broadcast(orderArrayToShare) //cannot broadcast RDD, need to be array/list
    //    println("=====Stage 1 Result:Sorted Array=====")
    //        printRDDIntArrayIntPrefix(sortedRDD, threshold)

    //Stage2 with Sort prefix ========================
    val Stage2Mapper = RDD.flatMap(tuple => stage2MapFuncSort(tuple._1, tuple._2, threshold, broadcastVar.value)) //.reduceByKey()
    //Stage2 Assume data sorted ========================
    //    val Stage2Mapper = RDD.flatMap(tuple => stage2MapFunc(tuple._1, tuple._2, threshold))
    println("=====Stage 2 Mapper=====")
    //printRDDIntArrayInt(Stage2Mapper)

    //    val Stage2Reducer = Stage2Mapper.aggregateByKey(ListBuffer.empty[Array[Int]])( //Array[(Int, List[Array[Int]])]
    //      (listBufferArray, array) => { listBufferArray += array; listBufferArray },
    //      (listBufferArray1, listBufferArray2) => { listBufferArray1.appendAll(listBufferArray2); listBufferArray1 })
    //      .mapValues(_.toList)

    //(Array[Int],index,value,value.size)
    val Stage2Reducer = Stage2Mapper.map(x => (x._1 -> (x._2, x._2(0), x._2.drop(1).to[scala.collection.mutable.Set], x._2.size - 1))).groupByKey()
    println("=====Stage 2 Reducer=====")

    //    for ((recordID, recordValue) <- Stage2Reducer) {
    //      print(recordID + ": ")
    //      for ((eachRecord, id, value, value_size) <- recordValue) {
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
    val Stage2Reducer_Step2 = Stage2Reducer.flatMap(prevIndexRecord => join(prevIndexRecord._2, threshold))
      .groupBy(v => (v._1, v._2, v._3)).keys
      .sortBy(index => (index._1, index._2, true))
      .map(x => "(" + x._1 + "," + x._2 + ")" + "\t" + x._3)
      .saveAsTextFile(outputPath)
    //      .foreach(println)
    //
    println("=====PairSet=====")

  }

  //(Array[Int],index,value,value.size)
  def join(line: Iterable[(Array[Int], Int, Set[Int], Int)], thres: Double): Set[(Int, Int, Double)] = {

    var result: Set[(Int, Int, Double)] = Set()
    val list = line.toArray
    val size = list.length - 1
    for (i <- 0 to size) {
      val record1 = list(i)
      val index_1 = record1._2
      val record1_value = record1._3
      for (j <- i + 1 to size) {
        val record2 = list(j)
        val index_2 = record2._2
        if (!PairSet.contains(index_1, index_2)) {
          PairSet.add(index_1, index_2)

          if (record2._4 >= thres * record1._4) {

            val record2_value = record2._3
            val total = record1_value ++ record2_value
            val less = record1_value.intersect(record2_value)
            val sim = less.size.toDouble / total.size.toDouble
            if (sim >= thres) {
              if (index_1 < index_2) {
                result.add((index_1, index_2, sim))
              } else {
                result.add((index_2, index_1, sim))
              }
            }
          }
        }
      }
    }
    return result
  }
}
