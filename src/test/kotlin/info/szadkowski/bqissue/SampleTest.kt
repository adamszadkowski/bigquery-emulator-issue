package info.szadkowski.bqissue

import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.cloud.NoCredentials
import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.storage.v1.*
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStubSettings
import com.google.common.util.concurrent.MoreExecutors
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import org.json.JSONArray
import org.json.JSONObject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isFalse
import strikt.assertions.isGreaterThan
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.apache.avro.Schema as ArvoSchema

@ExtendWith(RandomExtension::class)
class SampleTest(
    @RandomResolve(prefix = "testingbq") private val datasetId: String,
    @RandomResolve(prefix = "mytablename") private val tableId: String,
) {
    private val host = "http://localhost:9050"
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
        fun `read data using rest api`() {
            val query = "SELECT * FROM `$projectId.$datasetId.$tableId`"
            val loaded = service.query(QueryJobConfiguration.of(query))
                .values
                .map { Result(it.get("id").stringValue, it.get("otherProp").stringValue) }

            expectThat(loaded).containsExactly(
                Result("15432", "some value here"),
            )
        }

        @Test
        fun `read data using grpc api`() {
            val settings = BigQueryReadSettings.newBuilder()
                .setEndpoint("localhost:9060")
                .setCredentialsProvider { DummyCredentials() }
                .setTransportChannelProvider(
                    EnhancedBigQueryReadStubSettings.defaultGrpcTransportProviderBuilder()
                        .setChannelConfigurator { it.usePlaintext() }
                        .build()
                )
                .build()

            val client = BigQueryReadClient.create(settings)

            val options = ReadSession.TableReadOptions.newBuilder()
                .addSelectedFields("id")
                .addSelectedFields("otherProp")
                .build()

            val sessionBuilder = ReadSession.newBuilder()
                .setTable("projects/$projectId/datasets/$datasetId/tables/$tableId")
                .setDataFormat(DataFormat.AVRO)
                .setReadOptions(options)

            val request = CreateReadSessionRequest.newBuilder()
                .setParent("projects/$projectId")
                .setReadSession(sessionBuilder)
                .setMaxStreamCount(1)
                .build()

            val session = client.createReadSession(request)

            val reader = ResultReader(ArvoSchema.Parser().parse(session.avroSchema.schema))

            expectThat(session.streamsCount).isGreaterThan(0)

            val streamName = session.getStreams(0).name
            val readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(streamName).build()

            val loaded = client.readRowsCallable().call(readRowsRequest)
                .asSequence()
                .filter { it.hasAvroRows() }
                .flatMap { reader.processRows(it.avroRows) }
                .toList()

            expectThat(loaded).containsExactly(
                Result("15432", "some value here"),
            )
        }
    }

    @Nested
    inner class `grpc api saved data` {

        @BeforeEach
        fun `store data`() {
            val settings = BigQueryWriteSettings.newBuilder()
                .setEndpoint("localhost:9060")
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

                    latch.await(10, TimeUnit.SECONDS)
                }
        }

        @Test
        fun `read data using rest api`() {
            val query = "SELECT * FROM `$projectId.$datasetId.$tableId`"
            val loaded = service.query(QueryJobConfiguration.of(query))
                .values
                .map { Result(it.get("id").stringValue, it.get("otherProp").stringValue) }

            expectThat(loaded).containsExactly(
                Result("15432", "some value here"),
            )
        }
    }
}

class ResultReader(schema: ArvoSchema) {
    private val datumReader = GenericDatumReader<GenericRecord>(schema)
    private var decoder: BinaryDecoder? = null
    private var row: GenericRecord? = null

    fun processRows(rows: AvroRows): Sequence<Result> {
        decoder = DecoderFactory.get().binaryDecoder(rows.serializedBinaryRows.toByteArray(), decoder)
        return generateSequence {
            if (decoder!!.isEnd) null
            else {
                row = datumReader.read(row, decoder)
                row?.let {
                    Result(
                        id = it.get("id").toString(),
                        otherProp = it.get("otherProp").toString(),
                    )
                }
            }
        }
    }
}

data class Result(
    val id: String,
    val otherProp: String,
)
