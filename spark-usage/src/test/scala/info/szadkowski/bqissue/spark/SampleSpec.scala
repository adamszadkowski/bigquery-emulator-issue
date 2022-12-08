package info.szadkowski.bqissue.spark

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SampleSpec extends AnyFunSuite with Matchers {
  test("create spark session") {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("testing")
      .getOrCreate()

    import sparkSession.implicits._

    val rows = Seq(1, 2, 3, 4).toDF("number")
      .filter($"number" > lit(2))
      .collectAsList()

    rows should contain theSameElementsAs Seq(
      Row(3),
      Row(4)
    )
  }
}
