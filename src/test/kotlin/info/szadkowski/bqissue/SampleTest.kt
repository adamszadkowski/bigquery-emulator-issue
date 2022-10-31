package info.szadkowski.bqissue

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.*
import org.junit.jupiter.api.Test

class SampleTest {
    private val projectId = "<REPLACE_WITH_YOUR_PROJECT_ID>"
    private val datasetId = "testingbq"
    private val tableId = "mytablename"

    @Test
    fun `create dataset and table`() {
        val transportOptions = BigQueryOptions.getDefaultHttpTransportOptions()
            .toBuilder()
            .setConnectTimeout(6000)
            .setReadTimeout(6000)
            .build()

        val service = BigQueryOptions.newBuilder()
            .setTransportOptions(transportOptions)
            .setLocation("EU")
            .setCredentials(GoogleCredentials.getApplicationDefault())
            .build()
            .service

        service.create(
            DatasetInfo.newBuilder(projectId, datasetId)
                .setLocation("EU")
                .build()
        )

        service.create(
            TableInfo.of(
                TableId.of(projectId, datasetId, tableId),
                StandardTableDefinition.of(Schema.of(Field.of("id", StandardSQLTypeName.STRING)))
            )
        )
    }
}
