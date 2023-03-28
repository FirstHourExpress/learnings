package similator


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, rand, explode}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate}

import utils.Utils._

object Similator {
  // declaration and definition of function
  def createGSDF(
      spark: SparkSession,
      numRows: Int,
      numUniqueCropIds: Int,
      numUniqueSites: Int,
      startDate: LocalDate,
      endDate: LocalDate
  ): DataFrame = {

    // Create the gsDF data frame
    val gsRows: Seq[Row] = (1 to numRows).map { i =>
      val lat: Double =
        39.876887 + (i % numUniqueSites - numUniqueSites / 2) * 0.01
      val lon: Double =
        -105.03242 + (i % numUniqueSites - numUniqueSites / 2) * 0.01
      val cropId: Int = i % numUniqueCropIds + 1
      // format the dates, this converts from LocalDate to String
      val formatter: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MMM-dd")
      val plantingDate: String = startDate
        .plusDays(
          scala.util.Random.nextInt(
            endDate.toEpochDay.toInt - startDate.toEpochDay.toInt + 1
          )
        )
        .format(formatter)
      val emergenceDate: String =
        LocalDate
          .parse(plantingDate)
          .plusDays(scala.util.Random.nextInt(20) + 1)
          .format(formatter)
      Row(lat, lon, cropId, plantingDate, emergenceDate)
    }
    val gsSchema: StructType = StructType(
      Seq(
        StructField("lat", DoubleType, nullable = false),
        StructField("lon", DoubleType, nullable = false),
        StructField("cropID", IntegerType, nullable = false),
        StructField("plantingDate", StringType, nullable = false),
        StructField("emergenceDate", StringType, nullable = false)
      )
    )
    val gsDF: DataFrame =
      spark.createDataFrame(spark.sparkContext.parallelize(gsRows), gsSchema)

    gsDF
  }

  def createSoilDF(
      spark: SparkSession,
      gsDF: DataFrame,
      startDate: LocalDate,
      endDate: LocalDate
  ): DataFrame = {

    // format the dates, this converts from LocalDate to String
    val formatter: DateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MMM-dd")

    // without this line toDF won't work
    import spark.implicits._
    // Generate dates between startDate and endDate
    val minMaxWithRange = getDateRange(startDate, endDate).toDF("range")

    val dates = minMaxWithRange
      .withColumn("date", explode(col("range")))
      .drop("range")

    //  Join the sequence of data with gsDF dataframe
    val soilDF: DataFrame = gsDF
      .select(col("lat").as("lat"), col("lon").as("lon"))
      .distinct()
      .crossJoin(dates)
      .withColumn("soilMoisture", rand())
      .withColumn("soilTemp", rand(18) * 6 + 18)


    // Return the soilDF data frame
    soilDF
  }

}
