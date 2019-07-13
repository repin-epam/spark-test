import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}

object Task1 {
  // Finds 3 most popular hotels among couples
  val recordsLimit = 3
  val adultsCount = 2

  def performTask(df: DataFrame) = df
    .filter(col("srch_adults_cnt") === adultsCount)
    .groupBy("hotel_continent", "hotel_country", "hotel_market")
    .count()
    .orderBy(desc("count"))
    .limit(recordsLimit)
}
