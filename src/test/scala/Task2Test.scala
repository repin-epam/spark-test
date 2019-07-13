import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Task2Test extends FlatSpec with SparkSession with FileLoader {
  val testFile = "sample.csv"

  "Task 2 " should "find the most popular country where hotels are booked and searched from the same country" in {
    val df = loadFile(spark, testFile)
    val result = Task2.performTask(df).collectAsList().toList.map(_.toSeq.toList)
    assert(result.head == List(46))
  }

}