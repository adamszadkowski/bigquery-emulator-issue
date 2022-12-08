package info.szadkowski.bqissue.grpc

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.storage.v1.TableFieldSchema
import com.google.cloud.bigquery.storage.v1.TableSchema
import com.google.common.collect.ImmutableMap

object BqToBqStorageSchemaConverter {
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

    fun convertFieldSchema(field: Field): TableFieldSchema {
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