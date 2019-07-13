import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HelloSpark extends FileLoader {
  def main(args: Array[String]) {
    val session = SparkSession.builder
      .appName("Spark task 3")
      .master("local[*]")
      .getOrCreate

    val df: DataFrame = loadFile(session, args(0))

    val res = Task3.performTask(df)
      .write.mode(SaveMode.Overwrite).option("header", "true").csv(args(1))
  }
}