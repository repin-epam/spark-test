import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Task3 {
  // Find top 3 hotels where people with children are interested but not booked in the end
  val recordsLimit = 3
  val bookingsCount = 0
  val childrenMin = 0
  val adultsMin = 0

  def performTask(df: DataFrame) = df
    .filter(col("is_booking") === bookingsCount)
    .filter(col("srch_children_cnt") > childrenMin)
    .filter(col("srch_adults_cnt") > adultsMin)
    .groupBy("hotel_continent", "hotel_country", "hotel_market")
    .count()
    .orderBy(desc("count"))
    .limit(recordsLimit)
}
