# bigquery-emulator-issue

This repository contains code which shows failure of [bigquery-emulator](https://github.com/goccy/bigquery-emulator) when used with `google-cloud-bigquery` java library from Google.

## Handling gziped requests

When library sends requests with `Content-Encoding: gzip` header emulator is not handling this properly which gives an error:
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

Compression of requests cannot be turned off in `google-cloud-bigquery`, but on debug this behaviour can be altered by putting break point in `com.google.api.client.http.HttpRequest:889` and evaluating `encoding=null`.

## Missing type in response for table creation

When request is sent without compression there is another issue which appears. `google-cloud-bigquery` expects "type" key in response which informs what kind of "table" is it. This key is not sent by library by itself.

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