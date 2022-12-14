package info.szadkowski.bqissue.grpc

import com.google.cloud.NoCredentials
import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.storage.v1.*
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStubSettings
import info.szadkowski.bqissue.utils.RandomExtension
import info.szadkowski.bqissue.utils.RandomResolve
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isFalse
import strikt.assertions.isGreaterThan
import org.apache.avro.Schema as ArvoSchema

@ExtendWith(RandomExtension::class)
class BigQueryGrpcReadAPITest(
    @RandomResolve(prefix = "testingbq") private val datasetId: String,
    @RandomResolve(prefix = "mytablename") private val tableId: String,
) {
    private val restHost = "http://localhost:9050"
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
            .setHost(restHost)
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
    inner class `saved data` {

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
        fun `read data using grpc api`() {
            val settings = BigQueryReadSettings.newBuilder()
                .setEndpoint(grpcHost)
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
}
