import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{
  datediff,
  to_date,
  col,
  when,
  mean,
  filter
}

import java.time.{LocalDate, ZoneOffset, Period}

import similator.Similator._

object Emergence {
  def main(args: Array[String]) {

    // Create the SparkSession object
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Emergence")
      .master("local[*]")
      .getOrCreate()

    // Set the log level to WARN
    spark.sparkContext.setLogLevel("WARN")

    // Create the growth stage data frame with 10000 rows, 10 types of crops, and 100 sites
    val gsDF: DataFrame = createGSDF(
      spark,
      10000,
      10,
      100,
      LocalDate.of(2017, 1, 1),
      LocalDate.of(2023, 1, 1)
    )

    // Create the soil data frame
    val soilDF: DataFrame = createSoilDF(
      spark,
      gsDF,
      LocalDate.of(2017, 1, 1),
      LocalDate.of(2023, 1, 1)
    )

    // Join the soil data and growth stage data on the "lat" and "lon" columns
    val joinedDF = soilDF.join(gsDF, Seq("lat", "lon"))

    // Create a new column "emergenceDays" that is the difference between the "emergenceDate" and "plantingDate" columns
    val withEmergenceDaysDF = joinedDF.withColumn(
      "emergenceDays",
      datediff(
        to_date(col("emergenceDate"), "yyyy-MMM-dd"),
        to_date(col("plantingDate"), "yyyy-MMM-dd")
      )
    )

    // Create a new column "emergenceRate"
    // that indicates whether the emergence rate is fast, average, or slow
    val withEmergenceRate = withEmergenceDaysDF.withColumn(
      "emergenceRate",
      when(col("emergenceDays") > 10, "slow")
        .when(col("emergenceDays") < 5, "fast")
        .otherwise("fast")
    )

    // Group the result by the "cropID" and "emergenceRate" columns and aggregate on soil values
    val resultDF = withEmergenceRate
      .filter(
        to_date(col("date"), "yyyy-MMM-dd").between(
          to_date(col("plantingDate"), "yyyy-MMM-dd"),
          to_date(col("emergenceDate"), "yyyy-MMM-dd")
        )
      )
      .groupBy("cropID", "emergenceRate")
      .agg(
        mean("soilTemp").as("avgSoilTemp"),
        mean("soilMoisture").as("avgSoilMoisture")
      )

    // Select the "cropID", "emergenceRate", "avgSoilTemp", and "avgSoilMoisture" columns from the result data
    val selectedDF = resultDF.select(
      "cropID",
      "emergenceRrate",
      "avgSoilTemp",
      "avgSoilMoisture"
    )

    // Display the resulting data
    selectedDF.show()

    // spark.stop()
  }
}