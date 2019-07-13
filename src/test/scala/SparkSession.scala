import org.apache.spark.sql.SparkSession

trait SparkSession {
  lazy val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
}
