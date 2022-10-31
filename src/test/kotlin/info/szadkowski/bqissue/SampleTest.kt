package info.szadkowski.bqissue

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SampleTest {
    private val projectId = "<REPLACE_WITH_YOUR_PROJECT_ID>"
    private val datasetId = "testingbq"
    private val tableId = "mytablename"

    private lateinit var service: BigQuery

    @BeforeEach
    fun `configure bigquery`() {
        val transportOptions = BigQueryOptions.getDefaultHttpTransportOptions()
            .toBuilder()
            .setConnectTimeout(6000)
            .setReadTimeout(6000)
            .build()

        service = BigQueryOptions.newBuilder()
            .setTransportOptions(transportOptions)
            .setLocation("EU")
            .setCredentials(GoogleCredentials.getApplicationDefault())
            .build()
            .service
    }

    @AfterEach
    fun `remove dataset`() {
        service.delete(DatasetId.of(projectId, datasetId), BigQuery.DatasetDeleteOption.deleteContents())
    }

    @Test
    fun `create dataset and table`() {
        service.create(
            DatasetInfo.newBuilder(projectId, datasetId)
                .setLocation("EU")
                .build()
        )

        val schema = Schema.of(Field.of("id", StandardSQLTypeName.STRING))
        service.create(
            TableInfo.of(
                TableId.of(projectId, datasetId, tableId),
                StandardTableDefinition.of(schema)
            )
        )
    }
}
