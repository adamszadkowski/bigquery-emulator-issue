package info.szadkowski.bqissue.grpc

import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.cloud.NoCredentials
import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.storage.v1.*
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStubSettings
import com.google.common.collect.ImmutableMap
import com.google.common.util.concurrent.MoreExecutors
import info.szadkowski.bqissue.utils.RandomExtension
import info.szadkowski.bqissue.utils.RandomResolve
import org.json.JSONArray
import org.json.JSONObject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import strikt.api.expectThat
import strikt.assertions.isTrue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@ExtendWith(RandomExtension::class)
class BigQueryGrpcWriteAPITest(
    @RandomResolve(prefix = "testingbq") private val datasetId: String,
    @RandomResolve(prefix = "mytablename") private val tableId: String,
) {
    private val host = "http://localhost:9050"
    private val grpcHost = "localhost:9060"

    private val projectId = "test"

    private lateinit var service: BigQuery
    private lateinit var schema: Schema

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

        schema = Schema.of(
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
    inner class `grpc api saved data` {

        @Test
        fun `store data`() {
            val settings = BigQueryWriteSettings.newBuilder()
                .setEndpoint(grpcHost)
                .setCredentialsProvider { DummyCredentials() }
                .setTransportChannelProvider(
                    EnhancedBigQueryReadStubSettings.defaultGrpcTransportProviderBuilder()
                        .setChannelConfigurator { it.usePlaintext() }
                        .build()
                )
                .build()

            val client = BigQueryWriteClient.create(settings)

            val table = TableName.of(projectId, datasetId, tableId).toString()
            val tableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema)
            JsonStreamWriter.newBuilder(table, tableSchema, client).build()
                .use { streamWriter ->
                    val jsonArr = JSONArray().apply {
                        val record = JSONObject()
                        record.put("id", "15432")
                        record.put("otherProp", "some value here")
                        this.put(record)
                    }

                    val future = streamWriter.append(jsonArr)

                    val latch = CountDownLatch(1)
                    ApiFutures.addCallback(future, object : ApiFutureCallback<AppendRowsResponse> {
                        override fun onFailure(t: Throwable) {
                            throw t
                        }

                        override fun onSuccess(result: AppendRowsResponse) {
                            latch.countDown()
                        }

                    }, MoreExecutors.directExecutor())

                    expectThat(latch.await(2, TimeUnit.SECONDS)).isTrue()
                }
        }
    }
}

private object BqToBqStorageSchemaConverter {
    private val BQTableSchemaModeMap: ImmutableMap<Field.Mode, TableFieldSchema.Mode> = ImmutableMap.of(
        Field.Mode.NULLABLE, TableFieldSchema.Mode.NULLABLE,
        Field.Mode.REPEATED, TableFieldSchema.Mode.REPEATED,
        Field.Mode.REQUIRED, TableFieldSchema.Mode.REQUIRED
    )
    private val BQTableSchemaTypeMap: ImmutableMap<StandardSQLTypeName, TableFieldSchema.Type> =
        ImmutableMap.Builder<StandardSQLTypeName, TableFieldSchema.Type>()
            .put(StandardSQLTypeName.BOOL, TableFieldSchema.Type.BOOL)
            .put(StandardSQLTypeName.BYTES, TableFieldSchema.Type.BYTES)
            .put(StandardSQLTypeName.DATE, TableFieldSchema.Type.DATE)
            .put(StandardSQLTypeName.DATETIME, TableFieldSchema.Type.DATETIME)
            .put(StandardSQLTypeName.FLOAT64, TableFieldSchema.Type.DOUBLE)
            .put(StandardSQLTypeName.GEOGRAPHY, TableFieldSchema.Type.GEOGRAPHY)
            .put(StandardSQLTypeName.INT64, TableFieldSchema.Type.INT64)
            .put(StandardSQLTypeName.NUMERIC, TableFieldSchema.Type.NUMERIC)
            .put(StandardSQLTypeName.STRING, TableFieldSchema.Type.STRING)
            .put(StandardSQLTypeName.STRUCT, TableFieldSchema.Type.STRUCT)
            .put(StandardSQLTypeName.TIME, TableFieldSchema.Type.TIME)
            .put(StandardSQLTypeName.TIMESTAMP, TableFieldSchema.Type.TIMESTAMP)
            .build()

    fun convertTableSchema(schema: Schema): TableSchema {
        val result = TableSchema.newBuilder()
        for (i in 0 until schema.fields.size) {
            result.addFields(i, convertFieldSchema(schema.fields[i]))
        }
        return result.build()
    }

    private fun convertFieldSchema(field: Field): TableFieldSchema {
        var field = field
        val result = TableFieldSchema.newBuilder()
        if (field.mode == null) {
            field = field.toBuilder().setMode(Field.Mode.NULLABLE).build()
        }
        result.mode = BQTableSchemaModeMap[field.mode]
        result.name = field.name
        result.type = BQTableSchemaTypeMap[field.type.standardType]
        if (field.description != null) {
            result.description = field.description
        }
        if (field.subFields != null) {
            for (i in 0 until field.subFields.size) {
                result.addFields(i, convertFieldSchema(field.subFields[i]))
            }
        }
        return result.build()
    }
}
