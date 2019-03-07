package net.orfeon.cloud.dataflow.util.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class RecordToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordToTableRowConverter.class);

    public static TableRow convert(GenericRecord record) {
        TableRow row = new TableRow();
        for(final Schema.Field field : record.getSchema().getFields()) {
            row = setFieldValue(row, field.name(), field.schema(), record);
        }
        return row;
    }

    public static TableRow convert(SchemaAndRecord record) {
        return convert(record.getRecord());
    }

    public static TableSchema convertTableSchema(final GenericRecord record) {
        final List<TableFieldSchema> structFields = record.getSchema().getFields().stream()
                .map(field -> getFieldTableSchema(field.name(), field.schema(), record))
                .filter(fieldSchema -> fieldSchema != null)
                .collect(Collectors.toList());
        return new TableSchema().setFields(structFields);
    }

    // for BigQueryIO.Write.withSchemaFromView
    public static Map<String, String> convertTableSchema(final ValueProvider<String> output, final GenericRecord record) {
        final TableSchema tableSchema = convertTableSchema(record);
        final String json = new Gson().toJson(tableSchema);
        LOG.info(String.format("Spanner Query Result Schema Json: %s", json));
        final Map<String,String> map = new HashMap<>();
        map.put(output.get(), json);
        return map;
    }

    private static TableRow setFieldValue(TableRow row, final String fieldName, final Schema schema, final GenericRecord record) {
        if(record.get(fieldName) == null) {
            return row.set(fieldName, null);
        }
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return row.set(fieldName, record.get(fieldName).toString());
            case BYTES:
                final Map<String,Object> props = schema.getObjectProps();
                final int scale = props.containsKey("scale") ? Integer.valueOf(props.get("scale").toString()) : 0;
                final int precision = props.containsKey("precision") ? Integer.valueOf(props.get("precision").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    final ByteBuffer bytes = (ByteBuffer)record.get(fieldName);
                    final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes.array()).longValue(), scale);
                    return row.set(fieldName, bigDecimal);
                }
                final ByteBuffer bytes = (ByteBuffer)record.get(fieldName);
                return row.set(fieldName, bytes);
            case INT:
                final Long intValue = new Long((Integer)record.get(fieldName));
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    final LocalDate localDate = LocalDate.ofEpochDay(intValue);
                    return row.set(fieldName, localDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
                }
                return row.set(fieldName, intValue);
            case LONG:
                final Long longValue = (Long)record.get(fieldName);
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final Long seconds = schema.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longValue / 1000000 : longValue / 1000;
                    return row.set(fieldName, seconds);
                }
                return row.set(fieldName, longValue);
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return row.set(fieldName, record.get(fieldName));
            case FIXED:
                final GenericData.Fixed fixed = (GenericData.Fixed)record.get(fieldName);
                return row.set(fieldName, fixed.bytes());
            case RECORD:
                final TableRow childRow = convert((GenericRecord)record.get(fieldName));
                return row.set(fieldName, childRow);
            case MAP:
                final Map<Object, Object> map = (Map)record.get(fieldName);
                return row.set(fieldName, map.entrySet().stream()
                        .map(entry -> new TableRow()
                                    .set("key", entry.getKey() == null ? "" : entry.getKey().toString())
                                    .set("value", convertValue(schema.getValueType().getType(), entry.getValue())))
                        .collect(Collectors.toList()));
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setFieldValue(row, fieldName, childSchema, record);
                }
                return row;
            case ARRAY:
                return setArrayFieldValue(row, fieldName, schema.getElementType(), record);
            case NULL:
                // BigQuery ignores NULL value
                // https://cloud.google.com/bigquery/data-formats#avro_format
                return row;
            default:
                return row;
        }
    }

    private static TableRow setArrayFieldValue(TableRow row, final String fieldName, final Schema schema, final GenericRecord record) {
        boolean isNull = record.get(fieldName) == null;
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return row.set(fieldName, isNull ? null : ((List<Object>)record.get(fieldName)).stream()
                        .filter(utf8 -> utf8 != null)
                        .map(utf8 -> utf8.toString())
                        .collect(Collectors.toList()));
            case BYTES:
                final List<ByteBuffer> bytesValues = filterNull((List<ByteBuffer>)record.get(fieldName));
                final Map<String,Object> map = schema.getObjectProps();
                final int scale = map.containsKey("scale") ? Integer.valueOf(map.get("scale").toString()) : 0;
                final int precision = map.containsKey("precision") ? Integer.valueOf(map.get("precision").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    return row.set(fieldName, isNull ? null : bytesValues.stream()
                            .map(bytes -> BigDecimal.valueOf(new BigInteger(bytes.array()).longValue(), scale))
                            .collect(Collectors.toList()));
                }
                return row.set(fieldName, isNull ? null : bytesValues);
            case INT:
                final List<Integer> intValues =  filterNull((List<Integer>) record.get(fieldName));
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return row.set(fieldName, isNull ? null : intValues.stream()
                            .map(days -> LocalDate.ofEpochDay(days))
                            .map(localDate -> localDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
                            .collect(Collectors.toList()));
                } else {
                    return row.set(fieldName, intValues.stream()
                            .map(intValue -> intValue.longValue())
                            .collect(Collectors.toList()));
                }
            case LONG:
                final List<Long> longValues = filterNull((List<Long>)record.get(fieldName));
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return row.set(fieldName, isNull ? null : longValues.stream()
                            .map(longValue -> schema.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longValue / 1000000 : longValue / 1000)
                            .collect(Collectors.toList()));
                } else {
                    return row.set(fieldName, isNull ? null : longValues);
                }
            case FLOAT:
                return row.set(fieldName, isNull ? null : filterNull((List<Float>)record.get(fieldName)));
            case DOUBLE:
                return row.set(fieldName, isNull ? null : filterNull((List<Double>)record.get(fieldName)));
            case BOOLEAN:
                return row.set(fieldName, isNull ? null : filterNull((List<Boolean>)record.get(fieldName)));
            case FIXED:
                return row.set(fieldName, isNull ? null : ((List<GenericData.Fixed>)record.get(fieldName)).stream()
                        .filter(s -> s != null)
                        .map(s -> s.bytes())
                        .collect(Collectors.toList()));
            case RECORD:
                return row.set(fieldName, isNull ? null : ((List<GenericRecord>)record.get(fieldName)).stream()
                        .filter(r -> r != null)
                        .map(r -> convert(r))
                        .collect(Collectors.toList()));
            case MAP:
                // MAP in Array is not considerable ??
                row.set(fieldName, isNull ? null : ((List<Map<Object,Object>>)record.get(fieldName)).stream()
                        .filter(m -> m != null)
                        .map(m -> m.entrySet().stream()
                                .map(entry -> new TableRow()
                                        .set("key", entry.getKey() == null ? "" : entry.getKey().toString())
                                        .set("value", convertValue(schema.getValueType().getType(), entry.getValue())))
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList()));
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setArrayFieldValue(row, fieldName, childSchema, record);
                }
                return row;
            case ARRAY:
                // BigQuery does not support nested array.
                // https://cloud.google.com/bigquery/data-formats#avro_format
                return row;
            case NULL:
                // BigQuery ignore NULL value
                // https://cloud.google.com/bigquery/data-formats#avro_format
                return row;
            default:
                return row;
        }

    }

    private static TableFieldSchema getFieldTableSchema(final String fieldName, final Schema schema, final GenericRecord record) {
        return getFieldTableSchema(fieldName, schema, record, "REQUIRED");
    }

    private static TableFieldSchema getFieldTableSchema(final String fieldName, final Schema schema, final GenericRecord record, final String mode) {
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return new TableFieldSchema().setName(fieldName).setType("STRING").setMode(mode);
            case FIXED:
            case BYTES:
                final Map<String,Object> props = schema.getObjectProps();
                final int scale = props.containsKey("scale") ? Integer.valueOf(props.get("scale").toString()) : 0;
                final int precision = props.containsKey("precision") ? Integer.valueOf(props.get("precision").toString()) : 0;
                if (LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType("NUMERIC").setMode(mode);
                }
                return new TableFieldSchema().setName(fieldName).setType("BYTES").setMode(mode);
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType("DATE").setMode(mode);
                }
                return new TableFieldSchema().setName(fieldName).setType("INTEGER").setMode(mode);
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType("TIMESTAMP").setMode(mode);
                }
                return new TableFieldSchema().setName(fieldName).setType("INTEGER").setMode(mode);
            case FLOAT:
            case DOUBLE:
                return new TableFieldSchema().setName(fieldName).setType("FLOAT").setMode(mode);
            case BOOLEAN:
                return new TableFieldSchema().setName(fieldName).setType("BOOL").setMode(mode);
            case RECORD:
                if(record.get(fieldName) == null) {
                    return null;
                }
                final GenericRecord childRecord;
                if(record.get(fieldName) instanceof GenericRecord) {
                    childRecord = (GenericRecord)record.get(fieldName);
                } else {
                    if(((List<GenericRecord>)record.get(fieldName)).size() == 0) {
                        return null;
                    }
                    childRecord = ((List<GenericRecord>)record.get(fieldName)).get(0);
                }
                final List<TableFieldSchema> structFieldSchemas = childRecord.getSchema().getFields().stream()
                        .map(field -> getFieldTableSchema(field.name(), field.schema(), childRecord))
                        .collect(Collectors.toList());
                return new TableFieldSchema().setName(fieldName).setType("STRUCT").setFields(structFieldSchemas).setMode(mode);
            case MAP:
                final List<TableFieldSchema> mapFieldSchemas = ImmutableList.of(
                        new TableFieldSchema().setName("key").setType("STRING").setMode(mode),
                        new TableFieldSchema().setName("value").setType(convertType(schema.getValueType())).setMode("NULLABLE"));
                return new TableFieldSchema().setName(fieldName).setType("STRUCT").setFields(mapFieldSchemas).setMode("REPEATED");
            case UNION:
                for (final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return getFieldTableSchema(fieldName, childSchema, record, "NULLABLE");
                }
                throw new IllegalArgumentException();
            case ARRAY:
                return getFieldTableSchema(fieldName, schema.getElementType(), record).setMode("REPEATED");
            case NULL:
                // BigQuery ignores NULL value
                // https://cloud.google.com/bigquery/data-formats#avro_format
                throw new IllegalArgumentException();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static Object convertValue(final Schema.Type type, final Object value) {
        switch(type) {
            case ENUM:
            case STRING:
                return value == null ? null : value.toString();
            default:
                return value;
        }
    }

    private static String convertType(final Schema schema) {
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return "STRING";
            case FIXED:
            case BYTES:
                final Map<String,Object> props = schema.getObjectProps();
                final int scale = props.containsKey("scale") ? Integer.valueOf(props.get("scale").toString()) : 0;
                final int precision = props.containsKey("precision") ? Integer.valueOf(props.get("precision").toString()) : 0;
                if (LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    return "NUMERIC";
                }
                return "BYTES";
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return "DATE";
                }
                return "INTEGER";
            case LONG:
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return "TIMESTAMP";
                }
                return "INTEGER";
            case FLOAT:
            case DOUBLE:
                return "FLOAT";
            case BOOLEAN:
                return "BOOL";
            case MAP:
                return "STRUCT";
            case UNION:
                for (final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return convertType(childSchema);
                }
                throw new IllegalArgumentException();
            case RECORD:
                return "STRUCT";
            case ARRAY:
                return convertType(schema.getElementType());
            case NULL:
                return null;
            default:
                return null;
        }

    }

    private static <T> List<T> filterNull(final List<T> list) {
        return list.stream()
                .filter(value -> value != null)
                .collect(Collectors.toList());
    }

}
