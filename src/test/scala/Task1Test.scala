import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Task1Test extends FlatSpec with SparkSession with FileLoader {
  val testFile = "sample.csv"

  "Task 1 " should "Find 3 most popular hotels among couples" in {
    val df = loadFile(spark, testFile)
    val result = Task1.performTask(df).collectAsList().toList.map(_.toSeq.toList)
    assert(result == List(List(6, 105, 29, 91), List(2, 50, 368, 83), List(6, 105, 35, 80)))
  }
}
