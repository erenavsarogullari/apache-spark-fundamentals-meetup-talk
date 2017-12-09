package io.github.erenavsarogullari.spark.meetup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import io.github.erenavsarogullari.spark.meetup.common.ApplicationBase._

/**
  * Created by erenavsarogullari on 11/18/17.
  */
object Application_1_HowToCreateRDDs {

  val spark = SparkSession
    .builder()
    .appName("apache-spark-fundamentals")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]) {
    createNumberRDDByParallelizeCollection()
    createEmployeeRDDByLoadingFile()
    createNumberRDDAndGetEvenNumbers()
    sleep
  }

  private def createNumberRDDByParallelizeCollection(): Unit = {
    val numberRDD: RDD[Int] = spark.sparkContext.parallelize[Int](1 to 100)

    // Prints Number RDD Partition Count
    println(s"Number RDD Partition Count => ${numberRDD.getNumPartitions}")

    // Prints RDD dependencies
    println(s"RDD Dependencies => ${numberRDD.toDebugString}")

    // Prints first 5 rows of Number RDD
    println(s"Number RDD Content => ${numberRDD.take(5).mkString(", ")}")
  }

  private def createEmployeeRDDByLoadingFile(): Unit = {
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv", minPartitions = 3)

    // Prints Employee RDD Partition Count
    println(s"Employee RDD Partition Count => ${employeeRDD.getNumPartitions}")

    // Prints RDD dependencies
    println(s"RDD Dependencies => ${employeeRDD.toDebugString}")

    // Prints all rows of Employee RDD
    employeeRDD.collect().foreach(println)
  }

  private def createNumberRDDAndGetEvenNumbers(): Unit = {
    val numberRDD: RDD[Int] = spark.sparkContext.parallelize[Int](1 to 10)

    // Transforms numberRDD by adding 2 and filtering even numbers
    val evenNumberRDD = numberRDD.map(x => x + 2).filter(x => ((x % 2) == 0))

    // Prints RDD dependencies
    println(s"RDD Dependencies => ${evenNumberRDD.toDebugString}")

    // Prints all rows of Number RDD
    println(s"Number RDD Content => ${evenNumberRDD.collect.mkString(", ")}")
  }

}
