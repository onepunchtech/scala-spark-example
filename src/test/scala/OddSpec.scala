import org.scalatest._
import flatspec._
import matchers._
import org.apache.spark.sql.SparkSession

case class TestExample(
    name: String,
    input: List[String],
    expected: List[String]
)

class OddSpec extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {

  var ss: SparkSession = null

  override def beforeAll() = {
    ss = SparkSession.builder
      .master("local[*]")
      .appName("Test Odd Stuff")
      .getOrCreate()
  }

  override def afterAll() = {
    ss.stop()
  }

  val examples = List(
    TestExample(
      name = "simpleCase",
      input = List("1,2"),
      expected = List("1\t2")
    ),
    TestExample(
      name = "tabs and commas",
      input = List(
        "1,2",
        "2\t4"
      ),
      expected = List(
        "1\t2",
        "2\t4"
      )
    ),
    TestExample(
      name = "ignore headers",
      input = List(
        "header1,random",
        "1,2",
        "header1,random",
        "2\t4",
        "foo,bar",
        "xyz,h5"
      ),
      expected = List(
        "1\t2",
        "2\t4"
      )
    ),
    TestExample(
      name = "given example",
      input = List(
        "1,2",
        "1,3",
        "1,3",
        "2,4",
        "2,4",
        "2,4"
      ),
      expected = List(
        "1\t2",
        "2\t4"
      )
    ),
    TestExample(
      name = "different order same outcome",
      input = List(
        "1,3",
        "1,2",
        "2,4",
        "2,4",
        "1,3",
        "2,4"
      ),
      expected = List(
        "1\t2",
        "2\t4"
      )
    )
  )

  for {
    example <- examples
  } yield {
    it should example.name in {
      val rdd = ss.sparkContext.parallelize(example.input)
      val res = OddStuff.process(rdd).collect.toList
      res should equal(example.expected)
    }
  }
}
