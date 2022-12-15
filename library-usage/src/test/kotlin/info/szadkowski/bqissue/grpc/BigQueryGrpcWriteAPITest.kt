package info.szadkowski.bqissue.grpc

import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.cloud.NoCredentials
import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.storage.v1.*
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStubSettings
import com.google.common.util.concurrent.MoreExecutors
import info.szadkowski.bqissue.rest.BigQueryRestAPITest
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
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo
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
        service = BigQueryOptions.newBuilder()
            .setHost(host)
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
        fun `store data using default stream`() {
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

            expectThatDataWasWritten()
        }

        @Test
        fun `store data using created stream`() {
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

            val createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                .setParent(TableName.of(projectId, datasetId, tableId).toString())
                .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build())
                .build()

            val writeStream = client.createWriteStream(createWriteStreamRequest)
            JsonStreamWriter.newBuilder(writeStream.name, writeStream.tableSchema).build()
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
                    val finalizeResponse = client.finalizeWriteStream(streamWriter.streamName)
                    expectThat(finalizeResponse.rowCount).isEqualTo(1);
                }


            expectThatDataWasWritten()
        }

        private fun expectThatDataWasWritten() {
            val query = "SELECT * FROM `$projectId.$datasetId.$tableId`"
            val loaded = service.query(QueryJobConfiguration.of(query))
                .values
                .map { BigQueryRestAPITest.Result(it.get("id").stringValue, it.get("otherProp").stringValue) }

            expectThat(loaded).containsExactly(
                BigQueryRestAPITest.Result("15432", "some value here"),
            )
        }
    }
}

private object BqToBqStorageSchemaConverter {
    private val BQTableSchemaModeMap = mapOf(
        Field.Mode.NULLABLE to TableFieldSchema.Mode.NULLABLE,
        Field.Mode.REPEATED to TableFieldSchema.Mode.REPEATED,
        Field.Mode.REQUIRED to TableFieldSchema.Mode.REQUIRED,
    )
    private val BQTableSchemaTypeMap = mapOf(
        StandardSQLTypeName.BOOL to TableFieldSchema.Type.BOOL,
        StandardSQLTypeName.BYTES to TableFieldSchema.Type.BYTES,
        StandardSQLTypeName.DATE to TableFieldSchema.Type.DATE,
        StandardSQLTypeName.DATETIME to TableFieldSchema.Type.DATETIME,
        StandardSQLTypeName.FLOAT64 to TableFieldSchema.Type.DOUBLE,
        StandardSQLTypeName.GEOGRAPHY to TableFieldSchema.Type.GEOGRAPHY,
        StandardSQLTypeName.INT64 to TableFieldSchema.Type.INT64,
        StandardSQLTypeName.NUMERIC to TableFieldSchema.Type.NUMERIC,
        StandardSQLTypeName.STRING to TableFieldSchema.Type.STRING,
        StandardSQLTypeName.STRUCT to TableFieldSchema.Type.STRUCT,
        StandardSQLTypeName.TIME to TableFieldSchema.Type.TIME,
        StandardSQLTypeName.TIMESTAMP to TableFieldSchema.Type.TIMESTAMP,
    )

    fun convertTableSchema(schema: Schema): TableSchema =
        schema.fields.foldIndexed(initial = TableSchema.newBuilder()) { index, builder, field ->
            builder.addFields(index, convertFieldSchema(field))
        }.build()

    private fun convertFieldSchema(field: Field): TableFieldSchema = TableFieldSchema.newBuilder()
        .apply {
            mode = BQTableSchemaModeMap[field.mode ?: Field.Mode.NULLABLE]
            name = field.name
            type = BQTableSchemaTypeMap[field.type.standardType]
            field.description?.let { this.description = it }
            field.subFields?.forEachIndexed { i, f -> addFields(i, convertFieldSchema(f)) }
        }
        .build()
}
