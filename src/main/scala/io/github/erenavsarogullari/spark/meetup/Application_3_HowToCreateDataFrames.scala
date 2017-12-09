package io.github.erenavsarogullari.spark.meetup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import io.github.erenavsarogullari.spark.meetup.common.ApplicationBase._

/**
  * Created by erenavsarogullari on 11/18/17.
  */
object Application_3_HowToCreateDataFrames {

  val spark = SparkSession
    .builder()
    .appName("apache-spark-fundamentals")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]) {
    createEmployeeDataFrame
    createEmployeeDataFrameBySchema
    createEmployeeDataFrameByImplicitConversionOfRDD
    sleep
  }

  private def createEmployeeDataFrame(): Unit = {
    val employeeDF = spark.read.option("header", true).csv("src/main/resources/employees.csv")
    employeeDF.show()
  }

  private def createEmployeeDataFrameBySchema: Unit = {
    def dfSchema: StructType =
      StructType(
        Seq(
          StructField(name = "id", dataType = IntegerType, nullable = false),
          StructField(name = "name", dataType = StringType, nullable = false),
          StructField(name = "department", dataType = StringType, nullable = false)
        )
      )

    def employeeRDD: RDD[Row] = spark.sparkContext.parallelize(
      List(
        Row(1, "Employee_1", "Department_1"),
        Row(2, "Employee_2", "Department_2"))
    )

    val employeeDF = spark.createDataFrame(employeeRDD, dfSchema)

    employeeDF.explain()


    employeeDF.show()
  }

  private def createEmployeeDataFrameByImplicitConversionOfRDD: Unit = {
    import spark.implicits._
    val employeeRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/employees.csv")
    val employeeDF = employeeRDD.toDF
    employeeDF.show()
  }

}
