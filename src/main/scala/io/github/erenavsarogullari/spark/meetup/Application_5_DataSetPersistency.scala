package io.github.erenavsarogullari.spark.meetup

import org.apache.spark.sql.{Dataset, SparkSession}

import io.github.erenavsarogullari.spark.meetup.common.ApplicationBase._

/**
  * Created by erenavsarogullari on 11/18/17.
  */
object Application_5_DataSetPersistency {

  case class Employee(id: String, name: String, department: String)

  val spark = SparkSession
    .builder()
    .appName("apache-spark-fundamentals")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]) {
    cachedEmployeeDatasetSample
    sleep
  }

  /**
    * Creates filtered employees Dataset and shows how to cache
    */
  private def cachedEmployeeDatasetSample: Unit = {
    import spark.implicits._
    val employeeDS: Dataset[Employee] = spark.read.option("header", true).csv("src/main/resources/employees.csv").as[Employee]

    // Prints EMPLOYEE_3 and DEPARTMENT_7 and EMPLOYEE_9 rows...
    val filteredEmployeeDS =
      employeeDS
        .map(emp => Employee(emp.id, emp.name.toUpperCase(), emp.department.toUpperCase()))
        .filter(emp => (emp.name == "EMPLOYEE_3" || emp.department == "DEPARTMENT_7" || emp.name == "EMPLOYEE_9"))
        .cache()

    // Collect and prints filteredEmployeeRDD content
    filteredEmployeeDS
      .collect()
      .foreach(println)

    println("*********************************")

    import org.apache.spark.sql.functions.lit
    // Collect and prints all words in filteredEmployeeRDD content
    filteredEmployeeDS
      .withColumn("Phone number", lit("+00-123456789")) // creates column with default literal value
      .take(5)
      .foreach(println)
  }

}
