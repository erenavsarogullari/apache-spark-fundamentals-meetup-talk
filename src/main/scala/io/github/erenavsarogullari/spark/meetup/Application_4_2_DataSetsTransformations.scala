package io.github.erenavsarogullari.spark.meetup

import org.apache.spark.sql._

import io.github.erenavsarogullari.spark.meetup.common.ApplicationBase._

/**
  * Created by erenavsarogullari on 11/18/17.
  */
object Application_4_2_DataSetsTransformations {

  case class Employee(id: String, name: String, department: String)
  case class Address(id: String, address: String, city: String, country: String)

  val spark = SparkSession
    .builder()
    .appName("apache-spark-fundamentals")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]) {
    unionEmployeeDatasets
    joinEmployeeWithAddressDatasetsByInner
    joinEmployeeWithAddressDatasetsByLeftOuter
    intersectEmployeeDatasets
    repartitionAndCoalesceEmployeeDatasetPartitions
    sqlSample
    sleep
  }

  private def unionEmployeeDatasets: Unit = {
    import spark.implicits._
    val employeeDS = spark.read.option("header", true).csv("src/main/resources/employees.csv").as[Employee]
    val employeeDS2 = spark.read.option("header", true).csv("src/main/resources/employees2.csv").as[Employee]
    val allEmployeesDS = employeeDS.union(employeeDS2)
    allEmployeesDS.show()
  }

  private def joinEmployeeWithAddressDatasetsByInner: Unit = {
    import spark.implicits._
    val employeeDS = spark.read.option("header", true).csv("src/main/resources/employees.csv").as[Employee]
    val addressDS = spark.read.option("header", true).csv("src/main/resources/addresses.csv").as[Address]
    val employeesWithAddressDS = employeeDS.join(addressDS, Seq("id"))
    employeesWithAddressDS.show()
  }

  private def joinEmployeeWithAddressDatasetsByLeftOuter: Unit = {
    import spark.implicits._
    val employeeDS = spark.read.option("header", true).csv("src/main/resources/employees.csv").as[Employee]
    val addressDS = spark.read.option("header", true).csv("src/main/resources/addresses.csv").as[Address]
    val employeesWithAddressDS = employeeDS.join(addressDS, Seq("id"), "leftouter")
    employeesWithAddressDS.show()
  }

  private def intersectEmployeeDatasets: Unit = {
    import spark.implicits._
    val data1 = Seq[Employee](Employee("1","Employee_1","Department_1"), Employee("2","Employee_2","Department_2"))
    val data2 = Seq[Employee](Employee("3","Employee_3","Department_3"), Employee("2","Employee_2","Department_2"))
    val employeeDS1: Dataset[Employee] = spark.createDataset(data1)
    val employeeDS2: Dataset[Employee] = spark.createDataset(data2)
    employeeDS1.intersect(employeeDS2).show()
  }

  private def repartitionAndCoalesceEmployeeDatasetPartitions: Unit = {
    import spark.implicits._
    val data1 = Seq[Employee](Employee("1","Employee_1","Department_1"), Employee("2","Employee_2","Department_2"))
    val employeeDS: Dataset[Employee] = spark.createDataset(data1)

    val repartitionedRDD = employeeDS.repartition(5)

    // Prints Employee RDD Partition Count
    println(s"Employee RDD Partition Count after repartition => ${repartitionedRDD.rdd.getNumPartitions}")

    val coalescedRDD = repartitionedRDD.coalesce(3)

    // Prints Employee RDD Partition Count
    println(s"Employee RDD Partition Count after coalesce => ${coalescedRDD.rdd.getNumPartitions}")

    coalescedRDD.show()
  }

  private def sqlSample: Unit = {
    import spark.implicits._
    val employeeDS = spark.read.option("header", true).csv("src/main/resources/employees.csv").as[Employee]

    // Register Temp Table
    employeeDS.createTempView("OfficeEmployees")

    val filteredEmployeeDS = employeeDS.sqlContext.sql("select * from OfficeEmployees where id in (3,5)").as[Employee]
    filteredEmployeeDS.show()
  }

}
