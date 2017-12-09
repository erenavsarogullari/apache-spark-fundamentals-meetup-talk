package io.github.erenavsarogullari.spark.meetup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import io.github.erenavsarogullari.spark.meetup.common.ApplicationBase._

/**
  * Created by erenavsarogullari on 11/18/17.
  */
object Application_2_TransformationsAndActions {

  val spark = SparkSession
    .builder()
    .appName("apache-spark-fundamentals")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]) {
    createEmployeeRDDAndGetFilteredEmployees
    createEmployeeRDDAndGetFilteredWords
    createEmployeeRDDAndGetFirstEmployee
    createEmployeeRDDAndGetFirst5Employees
    createEmployeeRDDAndReturnAllEmployeesCount
    sleep
  }

  /**
    * Applies map, filter transformations and collect action on employeeRDD
    */
  private def createEmployeeRDDAndGetFilteredEmployees: Unit = {
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv")

    // Prints RDD dependencies
    println(s"RDD Dependencies => ${employeeRDD.toDebugString}")

    // Prints EMPLOYEE_3 and DEPARTMENT_7 rows...
    employeeRDD
      .map(_.toUpperCase)
      .filter(x => (x.contains("EMPLOYEE_3") || x.contains("DEPARTMENT_7")))
      .collect()
      .foreach(println)
  }

  /**
    * Applies flatMap, filter transformations and collect action on employeeRDD
    */
  private def createEmployeeRDDAndGetFilteredWords: Unit = {
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv")

    // Prints EMPLOYEE_3 and DEPARTMENT_7 records...
    employeeRDD
      .flatMap(_.toUpperCase.split(","))
      .filter(x => (x == "EMPLOYEE_3" || x == "DEPARTMENT_7"))
      .collect()
      .foreach(println)
  }

  /**
    * Gets first employee from employeeRDD
    */
  private def createEmployeeRDDAndGetFirstEmployee: Unit = {
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv")

    // Prints first row...
    employeeRDD
      .first()
      .foreach(print)
  }

  /**
    * Gets first 5 employee from employeeRDD
    */
  private def createEmployeeRDDAndGetFirst5Employees: Unit = {
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv")

    // Prints first 5 employees...
    employeeRDD
      .take(5)
      .foreach(println)
  }

  /**
    * Gets total count of employeeRDD
    */
  private def createEmployeeRDDAndReturnAllEmployeesCount: Unit = {
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv")

    // Prints total employees count...
    println(s"Total count of employeeRDD => ${employeeRDD.count()}")
  }


}
