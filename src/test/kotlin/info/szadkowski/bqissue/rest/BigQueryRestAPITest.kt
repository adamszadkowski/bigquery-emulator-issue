package info.szadkowski.bqissue.rest

import com.google.cloud.NoCredentials
import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.storage.v1.*
import info.szadkowski.bqissue.utils.RandomExtension
import info.szadkowski.bqissue.utils.RandomResolve
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isFalse

@ExtendWith(RandomExtension::class)
class BigQueryRestAPITest(
    @RandomResolve(prefix = "testingbq") private val datasetId: String,
    @RandomResolve(prefix = "mytablename") private val tableId: String,
) {
    private val host = "http://localhost:9050"
    private val projectId = "test"

    private lateinit var service: BigQuery

    @BeforeEach
    fun `create dataset and table`() {
        val transportOptions = BigQueryOptions.getDefaultHttpTransportOptions()
            .toBuilder()
            .setConnectTimeout(6000)
            .setReadTimeout(6000)
            .build()

        service = BigQueryOptions.newBuilder()
            .setHost(host)
            .setTransportOptions(transportOptions)
            .setLocation("EU")
            .setProjectId(projectId)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .service

        service.create(DatasetInfo.newBuilder(projectId, datasetId).setLocation("EU").build())

        val schema = Schema.of(
            Field.of("id", StandardSQLTypeName.STRING),
            Field.of("otherProp", StandardSQLTypeName.STRING),
        )
        service.create(TableInfo.of(TableId.of(projectId, datasetId, tableId), StandardTableDefinition.of(schema)))
    }

    @AfterEach
    fun `remove dataset`() {
        service.delete(DatasetId.of(projectId, datasetId), BigQuery.DatasetDeleteOption.deleteContents())
    }

    @Nested
    inner class `rest api saved data` {

        @BeforeEach
        fun `store data`() {
            val response = service.insertAll(
                InsertAllRequest.newBuilder(TableId.of(datasetId, tableId))
                    .addRow(mapOf("id" to "15432", "otherProp" to "some value here"))
                    .build()
            )

            expectThat(response.hasErrors()).isFalse()
        }

        @Test
        fun `read data`() {
            val query = "SELECT * FROM `$projectId.$datasetId.$tableId`"
            val loaded = service.query(QueryJobConfiguration.of(query))
                .values
                .map { Result(it.get("id").stringValue, it.get("otherProp").stringValue) }

            expectThat(loaded).containsExactly(
                Result("15432", "some value here"),
            )
        }
    }

    data class Result(
        val id: String,
        val otherProp: String,
    )
}
