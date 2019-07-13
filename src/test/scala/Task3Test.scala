import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Task3Test extends FlatSpec with SparkSession with FileLoader {
  val testFile = "sample.csv"

  "Task 3 " should "find top 3 hotels where people with children are interested but not booked in the end" in {
    val df = loadFile(spark, testFile)
    val result = Task3.performTask(df).collectAsList().toList.map(_.toSeq.toList)
    assert(result == List(List(2, 50, 368, 124), List(2, 50, 365, 64), List(2, 50, 366, 34)))
  }
}
