package com.epam.training.spark.core

import java.time.LocalDate
import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def MAX_COLUMNS = 7

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] =
    sc.textFile(rawDataPath)
      .filter(!_.startsWith("#"))
      .map(_.split(DELIMITER, MAX_COLUMNS).toList)
      .cache()

  def findErrors(rawData: RDD[List[String]]): List[Int] =
    rawData
      .map(
        fields =>
          fields
            .map(field =>
              if (field == null || field.isEmpty) 1 else 0))
      .reduce((list1, list2) => list1.zip(list2).map { case (a, b) => a + b })

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] =
    rawData
      .map(line => Climate(line(0), line(1), line(2), line(3), line(4), line(5), line(6)))

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] =
    climateData
      .filter(data => data.observationDate.getMonth.getValue == month
        && data.observationDate.getDayOfMonth == dayOfMonth
        && data.meanTemperature.value != Double.NaN)
      .map(_.meanTemperature.value)

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {

    def avgOnSameMonthAndDay(day: LocalDate): Double = {
      averageTemperature(climateData, day.getMonthValue, day.getDayOfMonth).mean()
    }

    def date = LocalDate.of(LocalDate.now().getYear, month, dayOfMonth)

    (avgOnSameMonthAndDay(date)
      + avgOnSameMonthAndDay(date.minusDays(1))
      + avgOnSameMonthAndDay(date.plusDays(1))) / 3.0

  }


}


