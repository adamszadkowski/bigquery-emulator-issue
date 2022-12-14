# bigquery-emulator-issue

This repository contains code which shows failure of [bigquery-emulator](https://github.com/goccy/bigquery-emulator)
when used with `google-cloud-bigquery` java library from Google.

## Current issues

## Handling of multiple read streams

Unfortunately spark integration with BigQuery requires reading multiple streams.

```scala
val rows = sparkSession.read
  .format("bigquery")
  .load(s"$projectId.$datasetId.$tableId")
  .collectAsList()
```

Code above doesn't work. There is a workaround, when `parallelism` can be set to `1`, but it would require change in
production code.

```scala
val rows = sparkSession.read
  .format("bigquery")
  .option("parallelism", 1) // required by bigquery-emulator
  .load(s"$projectId.$datasetId.$tableId")
  .collectAsList()
```

Even if technically this is possible to change this value - in practice it is very hard, to make that change in every
possible place. Additionally, it might cause some other issues.

## Support for partitioned tables

It looks like `bigquery-emulator` is not adding `_PARTITIONDATE` and `_PARTITIONTIME` columns to partitioned tables.
When table is created like this:

```scala
service.create(TableInfo.of(
  TableId.of(projectId, datasetId, tableId),
  StandardTableDefinition.newBuilder()
    .setSchema(schema)
    .setTimePartitioning(TimePartitioning.of(DAY))
    .build()))
```

Spark tries to read additional columns. It can be spotted in `bigquery-emulator` logs:

```
2022-12-14T11:08:47.941+0100	INFO	contentdata/repository.go:135		{"query": "SELECT `id`,`otherProp`,`_PARTITIONTIME`,`_PARTITIONDATE` FROM `mytablename` ", "values": []}
```

In spark on the other hand there is an error passed from `bigquery-emulator`:

```
Caused by: com.google.cloud.spark.bigquery.repackaged.io.grpc.StatusRuntimeException: UNKNOWN: failed to analyze: INVALID_ARGUMENT: Unrecognized name: _PARTITIONTIME [at 1:25]
	at com.google.cloud.spark.bigquery.repackaged.io.grpc.Status.asRuntimeException(Status.java:535)
	... 14 more
```

## Already solved issues

<s>

### Handling gziped requests

When library sends requests with `Content-Encoding: gzip` header emulator is not handling this properly which gives an
error:

```
invalid character '\x1f' looking for beginning of value
com.google.cloud.bigquery.BigQueryException: invalid character '\x1f' looking for beginning of value
	at app//com.google.cloud.bigquery.spi.v2.HttpBigQueryRpc.translate(HttpBigQueryRpc.java:115)
	at app//com.google.cloud.bigquery.spi.v2.HttpBigQueryRpc.create(HttpBigQueryRpc.java:170)
	at app//com.google.cloud.bigquery.BigQueryImpl$1.call(BigQueryImpl.java:269)
	at app//com.google.cloud.bigquery.BigQueryImpl$1.call(BigQueryImpl.java:266)
	at app//com.google.api.gax.retrying.DirectRetryingExecutor.submit(DirectRetryingExecutor.java:105)
	at app//com.google.cloud.RetryHelper.run(RetryHelper.java:76)
	at app//com.google.cloud.RetryHelper.runWithRetries(RetryHelper.java:50)
	at app//com.google.cloud.bigquery.BigQueryImpl.create(BigQueryImpl.java:265)
	at app//info.szadkowski.bqissue.SampleTest.create dataset and table(SampleTest.kt:41)
```

And in logs of `bigquery-emulator` can be seen:

```
2022-10-31T13:33:43.563+0100	ERROR	server/handler.go:608	invalid	{"error": "invalid: invalid character '\\x1f' looking for beginning of value"}
```

`\\x1f` is a first byte when request is gzipped.

Compression of requests cannot be turned off in `google-cloud-bigquery`, but on debug this behaviour can be altered by
putting break point in `com.google.api.client.http.HttpRequest:889` and evaluating `encoding=null`.

### Missing type in response for table creation

When request is sent without compression there is another issue which appears. `google-cloud-bigquery` expects "type"
key in response which informs what kind of "table" is it. This key is not sent by library by itself.

```
Empty enum constants not allowed.
java.lang.IllegalArgumentException: Empty enum constants not allowed.
	at com.google.cloud.StringEnumType.valueOf(StringEnumType.java:66)
	at com.google.cloud.bigquery.TableDefinition$Type.valueOf(TableDefinition.java:102)
	at com.google.cloud.bigquery.TableDefinition.fromPb(TableDefinition.java:159)
	at com.google.cloud.bigquery.TableInfo$BuilderImpl.<init>(TableInfo.java:195)
	at com.google.cloud.bigquery.Table.fromPb(Table.java:630)
	at com.google.cloud.bigquery.BigQueryImpl.create(BigQueryImpl.java:291)
	at info.szadkowski.bqissue.SampleTest.create dataset and table(SampleTest.kt:48)
	...
```

</s>