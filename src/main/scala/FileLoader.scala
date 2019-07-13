import org.apache.spark.sql.SparkSession

trait FileLoader {
  def loadFile(session: SparkSession, fileName: String) = session.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(fileName)

}
