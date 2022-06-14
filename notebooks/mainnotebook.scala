// Databricks notebook source
// MAGIC %md
// MAGIC #Mounting data-directory if not exist

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "XXX",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="slizhscope",key="storagekey"),
  "fs.azure.account.oauth2.client.endpoint" -> "XXX")
val mountedDirectory = "/mnt/data"
if(!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mountedDirectory)){
  dbutils.fs.mount(
  source = "abfss://data@stslizhwesteurope.dfs.core.windows.net/",
  mountPoint = mountedDirectory,
  extraConfigs = configs)
}

// COMMAND ----------

// MAGIC %md
// MAGIC #Saving data to delta-tables

// COMMAND ----------

val hotelsWeatherDf = spark.read.parquet("/mnt/data/hotel-weather")
hotelsWeatherDf.write.format("delta").mode("overwrite").save("/tmp/delta/hotels-weather")
val expediaDf = spark.read.format("avro").load("/mnt/data/expedia")
expediaDf.write.format("delta").mode("overwrite").save("/tmp/delta/expedia")


// COMMAND ----------

// MAGIC %md
// MAGIC #First SQL query: Top 10 hotels with max absolute temperature difference by month.

// COMMAND ----------

val queryResult = spark.sql("""
SELECT 
  name,
  address,
  city,
  country,
  geoHash,
  id,
  latitude,
  longitude,
  tmpr_diff,
  year,
  month
FROM (SELECT name,
             address,
             city,
             country,
             geoHash,
             id,
             latitude,
             longitude,
             MAX(avg_tmpr_c) - MIN(avg_tmpr_c) AS tmpr_diff,
             year,
             month,
             ROW_NUMBER() OVER(PARTITION BY year, month ORDER BY (MAX(avg_tmpr_c) - MIN(avg_tmpr_c)) DESC) AS number
      FROM delta.`/tmp/delta/hotels-weather`
      GROUP BY name,
               address,
               city,
               country,
               geoHash,
               id,
               latitude,
               longitude,
               year,
               month
      ORDER BY year,
               month,
               tmpr_diff DESC
      )
WHERE number <= 10
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ###Saving query result:

// COMMAND ----------

val path = "/mnt/data/result/1"
queryResult.write.mode("overwrite").parquet(path)

// COMMAND ----------

// MAGIC %md
// MAGIC #Second SQL query: Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Preparing data: counting visits for each hotel by month

// COMMAND ----------

import io.delta.implicits._
import scala.collection.mutable.ListBuffer 
import java.time.LocalDate
import org.apache.spark.sql.functions._
val df = spark.read.delta("/tmp/delta/expedia")
val dateRegex = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
val visitsDf = df.filter(col("srch_ci").rlike(dateRegex) && col("srch_co").rlike(dateRegex))
  .flatMap(x => {
    val hotelId : Long = x.getLong(19)
    val checkinDate = LocalDate.parse(x.getString(12)).withDayOfMonth(1)
    val checkoutDate = LocalDate.parse(x.getString(13)).withDayOfMonth(1)
    var resultBuffer = new ListBuffer[(Long, Int, Int)]()
    resultBuffer += ((hotelId, checkinDate.getYear(), checkinDate.getMonth().getValue()))
    var date = checkinDate
    while (date.isBefore(checkoutDate)){
      
      date = date.plusMonths(1)
      resultBuffer += ((hotelId, date.getYear(), date.getMonth().getValue()))
    }
    resultBuffer.toList
  })
  .withColumnRenamed("_1", "hotelId").withColumnRenamed("_2", "year").withColumnRenamed("_3", "month")
  .groupBy("hotelId","year","month").count().orderBy("year","month", "count")
  .createOrReplaceTempView("counted_visits")

// COMMAND ----------

// MAGIC %md
// MAGIC ###Selecting top 10 busy (e.g., with the biggest visits count) hotels for each month.

// COMMAND ----------

val queryResult = spark.sql("""
SELECT 
  hotelId,
  name,
  hotels_visit.year,
  hotels_visit.month,
  visits
FROM (
  SELECT 
    hotelId,
    year,
    month,
    count AS visits,
    ROW_NUMBER() OVER(PARTITION BY year, month ORDER BY count DESC) AS number
  FROM counted_visits
) AS hotels_visit
INNER JOIN delta.`/tmp/delta/hotels-weather` AS hotels_weather
ON hotelId = hotels_weather.id
WHERE number <= 10
GROUP BY hotelId, name, hotels_visit.year, hotels_visit.month, visits
""")


// COMMAND ----------

// MAGIC %md
// MAGIC ###Saving query result:

// COMMAND ----------

val path = "/mnt/data/result/2"
queryResult.write.mode("overwrite").parquet(path)

// COMMAND ----------

// MAGIC %md
// MAGIC #Third SQL query: For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

// COMMAND ----------

// MAGIC %md
// MAGIC ####Preparing data: filter extended (more than 7 days) stays

// COMMAND ----------

import org.apache.spark.sql.functions._
val df = spark.read.delta("/tmp/delta/expedia")
val dateRegex = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
val extendedVisits = df.filter(col("srch_ci").rlike(dateRegex) && col("srch_co").rlike(dateRegex))
                    .withColumn("date_diff", datediff(col("srch_co"), col("srch_ci")))
                    .filter(col("date_diff") >= 7)
                    .withColumnRenamed("id", "visit_id")
                    .createOrReplaceTempView("extended_visits")


// COMMAND ----------

// MAGIC %md
// MAGIC ###Calculating weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

// COMMAND ----------

val queryResult = spark.sql("""
SELECT *
FROM(
     SELECT visit_id,
            name,
            id AS hotel_id,
            latitude,
            longitude,
            srch_ci,
            srch_co,
            date_diff,
            AVG(avg_tmpr_c) OVER (PARTITION BY visit_id ORDER BY visit_id ASC) AS avg_tmpr_during_stay,
            FIRST_VALUE(avg_tmpr_c) OVER (PARTITION BY visit_id ORDER BY visit_id ASC) -
            LAST_VALUE(avg_tmpr_c) OVER (PARTITION BY visit_id ORDER BY visit_id ASC) AS weather_trend
     FROM delta.`/tmp/delta/hotels-weather` AS hotels_weather
     JOIN extended_visits
     ON hotels_weather.id = extended_visits.hotel_id
     AND wthr_date >= srch_ci
     AND wthr_date <= srch_co
) 
GROUP BY visit_id,
         name,
         hotel_id,
         latitude,
         longitude,
         srch_ci,
         srch_co,
         date_diff,
         avg_tmpr_during_stay,
         weather_trend
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ###Saving query result:

// COMMAND ----------

val path = "/mnt/data/result/3"
queryResult.write.mode("overwrite").parquet(path)
