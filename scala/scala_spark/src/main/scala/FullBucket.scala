import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

object FullBucket {
  def main(args: Array[String]): Unit = {
    val spark = Configurer.dockerSparkEnvironmentMasterWithWorkers("Bronze to Silver")

    val bucketPath = "s3a://bronze/"
    val runDate = LocalDateTime.now().toLocalDate.toString

    val waterline =
      args.sliding(2, 2).collect {
        case Array("--waterline", ts) => ts
      }.toList.headOption.getOrElse("00000000_000000")

    val dmToDecimalUDF = udf(dmToDecimal _)

    val rawDf = spark.read
      .option("header", true)
      .option("inferSchema", false)
      .csv(bucketPath)

    val splitSkipper = split(col("skipper_boat"), " ")
    val splitCol = split(col("nationality_sail"), " ")

    val dfWithFileTs = rawDf
      .withColumn("date_part", regexp_extract(input_file_name(), "leaderboard_(\\d{8})_(\\d{6})\\.csv", 1))
      .withColumn("time_part", regexp_extract(input_file_name(), "leaderboard_(\\d{8})_(\\d{6})\\.csv", 2))
      .withColumn("file_ts", concat(col("date_part"), lit("_"), col("time_part")))
      .filter(col("file_ts") > lit(waterline))

    val newWaterline: String = dfWithFileTs
      .agg(max("file_ts").alias("max_ts"))
      .collect()
      .map(_.getAs[String]("max_ts"))
      .headOption
      .getOrElse(waterline)

    val df = dfWithFileTs
      .select(
        col("rank"),
        dmToDecimalUDF(col("latitude")).alias("latitude"),
        dmToDecimalUDF(col("longitude")).alias("longitude"),
        regexp_replace(col("heading_30min"), "°", "").cast(IntegerType).alias("heading_30min"),
        regexp_replace(col("speed_30min"), "[^0-9\\.]", "").cast(FloatType).alias("speed_30min"),
        regexp_replace(col("avg_speed_30min"), "[^0-9\\.]", "").cast(FloatType).alias("avg_speed_30min"),
        regexp_replace(col("distance_30min"), "[^0-9\\.]", "").cast(FloatType).alias("distance_30min"),
        regexp_replace(col("heading_last_report"), "°", "").cast(IntegerType).alias("heading_last_report"),
        regexp_replace(col("speed_last_report"), "[^0-9\\.]", "").cast(FloatType).alias("speed_last_report"),
        regexp_replace(col("avg_speed_last_report"), "[^0-9\\.]", "").cast(FloatType).alias("avg_speed_last_report"),
        regexp_replace(col("distance_last_report"), "[^0-9\\.]", "").cast(FloatType).alias("distance_last_report"),
        regexp_replace(col("heading_24h"), "°", "").cast(IntegerType).alias("heading_24h"),
        regexp_replace(col("speed_24h"), "[^0-9\\.]", "").cast(FloatType).alias("speed_24h"),
        regexp_replace(col("avg_speed_24h"), "[^0-9\\.]", "").cast(FloatType).alias("avg_speed_24h"),
        regexp_replace(col("distance_24h"), "[^0-9\\.]", "").cast(FloatType).alias("distance_24h"),
        regexp_replace(col("dtf"), "[^0-9\\.]", "").cast(FloatType).alias("dtf"),
        regexp_replace(col("dtl"), "[^0-9\\.]", "").cast(FloatType).alias("dtl"),
        when(col("skipper_boat").startsWith("Jean Le"), lit("Jean Le Cam"))
          .when(col("skipper_boat").startsWith("Denis Van"), lit("Denis Van Weynbergh"))
          .otherwise(concat_ws(" ", splitSkipper.getItem(0), splitSkipper.getItem(1))).alias("Sailor"),
        when(col("skipper_boat").startsWith("Jean Le Cam"),
          trim(substring(col("skipper_boat"), 13, 1000)))
          .when(col("skipper_boat").startsWith("Denis Van"),
            trim(substring(col("skipper_boat"), 19, 1000)))
          .otherwise(concat_ws(" ", slice(splitSkipper, 3, 1000))).alias("Team"),
        splitCol.getItem(0).alias("Nation"),
        concat_ws(" ", splitCol.getItem(1), splitCol.getItem(2)).alias("Sail"),
        to_timestamp(concat(col("date_part"), lit(" "), substring(col("time"), 0, 5)), "yyyyMMdd HH:mm")
          .alias("Time in France")
      )

    df.write
      .mode("append")
      .partitionBy("Time in France")
      .parquet("s3a://silver/leaderboard")


    val waterlinePath = new Path("s3a://misc/waterline.txt")
    val fs = FileSystem.get(waterlinePath.toUri, spark.sparkContext.hadoopConfiguration)

    if (fs.exists(waterlinePath)) {
      fs.delete(waterlinePath, false)
    }

    val outputStream = fs.create(waterlinePath)
    val writer = new PrintWriter(outputStream)
    try {
      writer.write(newWaterline)
    } finally {
      writer.close()
      outputStream.close()
    }


    spark.stop()
  }

  def dmToDecimal(coord: String): Float = {
    try {
      if (coord == null || coord.isEmpty) return Float.NaN
      val degMin = coord.split("°")
      if (degMin.length < 2) return Float.NaN
      val deg = degMin(0).toFloat
      val minH = degMin(1)
      if (minH.isEmpty) return Float.NaN
      val hemi = minH.last
      val minStr = minH.takeWhile(c => c.isDigit || c == '.')
      if (minStr.isEmpty) return Float.NaN
      val min = minStr.toFloat
      val decimal = deg + min / 60.0f
      if (hemi == 'S' || hemi == 'W') -decimal else decimal
    } catch {
      case _: Exception => Float.NaN
    }
  }
}