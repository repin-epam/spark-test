import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}

object Task2 {
  // Find the most popular country where hotels are booked and searched from the same country
  val recordsLimit = 1

  def performTask(df: DataFrame) = df
    .filter(col("hotel_country") === col("user_location_country"))
    .groupBy("hotel_country")
    .count()
    .orderBy(desc("count"))
    .select(col("hotel_country"))
    .limit(recordsLimit)
}
