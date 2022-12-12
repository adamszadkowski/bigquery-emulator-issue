package info.szadkowski.bqissue.spark

import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.JavaConverters._

class BigQueryReadSpec extends AnyFunSuite with Matchers with BeforeAndAfter {
  private val projectId = "test"
  private var datasetId: String = _
  private var tableId: String = _

  private lazy val service = BigQueryOptions.newBuilder()
    .setLocation("EU")
    .setProjectId(projectId)
    .build()
    .getService

  private lazy val sparkSession = SparkSession.builder()
    .master("local")
    .appName("testing")
    .getOrCreate()

  before {
    val random = UUID.randomUUID().toString.replaceAll("-", "")
    datasetId = s"testingbq$random"
    tableId = s"mytablename$random"
    service.create(DatasetInfo.newBuilder(projectId, datasetId).setLocation("EU").build())

    val schema = Schema.of(
      Field.of("id", StandardSQLTypeName.STRING),
      Field.of("otherProp", StandardSQLTypeName.STRING),
    )
    service.create(TableInfo.of(TableId.of(projectId, datasetId, tableId), StandardTableDefinition.of(schema)))
    service.insertAll(
      InsertAllRequest.newBuilder(TableId.of(datasetId, tableId))
        .addRow(Map("id" -> "15432", "otherProp" -> "some value here").asJava)
        .build()
    )
  }

  after {
    service.delete(DatasetId.of(projectId, datasetId), BigQuery.DatasetDeleteOption.deleteContents())
  }

  test("loading any data") {
    val rows = sparkSession.read
      .format("bigquery")
      .load(s"$projectId.$datasetId.$tableId")
      .collectAsList()
    rows should contain theSameElementsAs Seq(
      Row("15432", "some value here")
    )
  }
}
