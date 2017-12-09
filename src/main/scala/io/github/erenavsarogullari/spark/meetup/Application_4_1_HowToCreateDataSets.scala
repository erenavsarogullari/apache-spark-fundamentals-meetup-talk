package io.github.erenavsarogullari.spark.meetup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import io.github.erenavsarogullari.spark.meetup.common.ApplicationBase._

/**
  * Created by eren.avsarogullari on 11/18/17.
  */
object Application_4_1_HowToCreateDataSets {

  case class Employee(id: String, name: String, department: String)

  val spark = SparkSession
    .builder()
    .appName("apache-spark-fundamentals")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]) {
    createEmployeeDatasetFromDataFrame
    createDatasetFromCollection
    createDatasetFromRDD
    sleep
  }

  private def createEmployeeDatasetFromDataFrame: Unit = {
    import spark.implicits._
    val employeeDF: DataFrame = spark.read.option("header", true).csv("src/main/resources/employees.csv")
    val employeeDS: Dataset[Employee] = employeeDF.as[Employee]
    employeeDS.show()
  }

  private def createDatasetFromCollection: Unit = {
    import spark.implicits._
    val data = Seq[Employee](Employee("1","Employee_1","Department_1"), Employee("2","Employee_2","Department_2"))
    val employeeDS: Dataset[Employee] = spark.createDataset(data)
    employeeDS.show()
  }

  private def createDatasetFromRDD: Unit = {
    import spark.implicits._
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv")
    val employeeDS: Dataset[String] = spark.createDataset(employeeRDD)
    employeeDS.show()
  }

}
