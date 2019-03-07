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
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class RecordToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordToTableRowConverter.class);

    private enum TableRowFieldType {
        STRING,
        BYTES,
        INT64,
        FLOAT64,
        NUMERIC,
        BOOL,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        GEOGRAPHY,
        ARRAY,
        STRUCT
    }

    private enum TableRowFieldMode {
        REQUIRED,
        NULLABLE,
        REPEATED
    }

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

    public static TableSchema convertTableSchema(final Schema schema) {
        final List<TableFieldSchema> structFields = schema.getFields().stream()
                .map(field -> getFieldTableSchema(field.name(), field.schema()))
                .filter(fieldSchema -> fieldSchema != null)
                .collect(Collectors.toList());
        return new TableSchema().setFields(structFields);
    }

    public static TableSchema convertTableSchema(final GenericRecord record) {
        return convertTableSchema(record.getSchema());
    }

    // for BigQueryIO.Write.withSchemaFromView
    public static Map<String, String> convertTableSchema(final ValueProvider<String> output, final Schema schema) {
        final TableSchema tableSchema = convertTableSchema(schema);
        final String json = new Gson().toJson(tableSchema);
        LOG.info(String.format("Spanner Query Result Schema Json: %s", json));
        final Map<String,String> map = new HashMap<>();
        map.put(output.get(), json);
        return map;
    }

    // for BigQueryIO.Write.withSchemaFromView
    public static Map<String, String> convertTableSchema(final ValueProvider<String> output, final GenericRecord record) {
        return convertTableSchema(output, record.getSchema());
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
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    LocalTime time = LocalTime.ofNanoOfDay(intValue * 1000 * 1000);
                    return row.set(fieldName, time.format(DateTimeFormatter.ISO_LOCAL_TIME));
                }
                return row.set(fieldName, intValue);
            case LONG:
                final Long longValue = (Long)record.get(fieldName);
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return row.set(fieldName, longValue / 1000);
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return row.set(fieldName, longValue / 1000000);
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    LocalTime time = LocalTime.ofNanoOfDay(longValue * 1000);
                    return row.set(fieldName, time.format(DateTimeFormatter.ISO_LOCAL_TIME));
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
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return row.set(fieldName, intValues.stream()
                            .map(millisec -> millisec.longValue())
                            .map(millisec -> LocalTime.ofNanoOfDay(millisec * 1000 * 1000))
                            .map(localTime -> localTime.format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .collect(Collectors.toList()));
                } else {
                    return row.set(fieldName, intValues.stream()
                            .map(intValue -> intValue.longValue())
                            .collect(Collectors.toList()));
                }
            case LONG:
                final List<Long> longValues = filterNull((List<Long>)record.get(fieldName));
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return row.set(fieldName, isNull ? null : longValues.stream()
                            .map(millisec -> millisec / 1000)
                            .collect(Collectors.toList()));
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return row.set(fieldName, isNull ? null : longValues.stream()
                            .map(microsec -> microsec / 1000000)
                            .collect(Collectors.toList()));
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return row.set(fieldName, isNull ? null : longValues.stream()
                            .map(microsec -> LocalTime.ofNanoOfDay(microsec * 1000))
                            .map(localTime -> localTime.format(DateTimeFormatter.ISO_LOCAL_TIME))
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

    private static TableFieldSchema getFieldTableSchema(final String fieldName, final Schema schema) {
        return getFieldTableSchema(fieldName, schema, TableRowFieldMode.REQUIRED);
    }

    private static TableFieldSchema getFieldTableSchema(final String fieldName, final Schema schema, final TableRowFieldMode mode) {
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.STRING.name()).setMode(mode.name());
            case FIXED:
            case BYTES:
                final Map<String,Object> props = schema.getObjectProps();
                final int scale = props.containsKey("scale") ? Integer.valueOf(props.get("scale").toString()) : 0;
                final int precision = props.containsKey("precision") ? Integer.valueOf(props.get("precision").toString()) : 0;
                if (LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.NUMERIC.name()).setMode(mode.name());
                }
                return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.BYTES.name()).setMode(mode.name());
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.DATE.name()).setMode(mode.name());
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.TIME.name()).setMode(mode.name());
                }
                return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.INT64.name()).setMode(mode.name());
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.TIMESTAMP.name()).setMode(mode.name());
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.TIME.name()).setMode(mode.name());
                }
                return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.INT64.name()).setMode(mode.name());
            case FLOAT:
            case DOUBLE:
                return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.FLOAT64.name()).setMode(mode.name());
            case BOOLEAN:
                return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.BOOL.name()).setMode(mode.name());
            case RECORD:
                final List<TableFieldSchema> structFieldSchemas = schema.getFields().stream()
                        .map(field -> getFieldTableSchema(field.name(), field.schema()))
                        .collect(Collectors.toList());
                return new TableFieldSchema().setName(fieldName).setType(TableRowFieldType.STRUCT.name()).setFields(structFieldSchemas).setMode(mode.name());
            case MAP:
                final List<TableFieldSchema> mapFieldSchemas = ImmutableList.of(
                        new TableFieldSchema().setName("key").setType(TableRowFieldType.STRING.name()).setMode(mode.name()),
                        addMapValueType(new TableFieldSchema().setName("value"), schema.getValueType()));
                return new TableFieldSchema()
                        .setName(fieldName)
                        .setType(TableRowFieldType.STRUCT.name())
                        .setFields(mapFieldSchemas)
                        .setMode(TableRowFieldMode.REPEATED.name());
            case UNION:
                for (final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return getFieldTableSchema(fieldName, childSchema, TableRowFieldMode.NULLABLE);
                }
                throw new IllegalArgumentException();
            case ARRAY:
                return getFieldTableSchema(fieldName, schema.getElementType()).setMode(TableRowFieldMode.REPEATED.name());
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

    private static TableFieldSchema addMapValueType(final TableFieldSchema fieldSchema, final Schema schema) {
        return addMapValueType(fieldSchema, schema, TableRowFieldMode.REQUIRED);
    }

    private static TableFieldSchema addMapValueType(final TableFieldSchema fieldSchema, final Schema schema, final TableRowFieldMode mode) {
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return fieldSchema.setType(TableRowFieldType.STRING.name()).setMode(mode.name());
            case FIXED:
            case BYTES:
                final Map<String,Object> props = schema.getObjectProps();
                final int scale = props.containsKey("scale") ? Integer.valueOf(props.get("scale").toString()) : 0;
                final int precision = props.containsKey("precision") ? Integer.valueOf(props.get("precision").toString()) : 0;
                if (LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    return fieldSchema.setType(TableRowFieldType.NUMERIC.name()).setMode(mode.name());
                }
                return fieldSchema.setType(TableRowFieldType.BYTES.name()).setMode(mode.name());
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return fieldSchema.setType(TableRowFieldType.DATE.name()).setMode(mode.name());
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return fieldSchema.setType(TableRowFieldType.TIME.name()).setMode(mode.name());
                }
                return fieldSchema.setType(TableRowFieldType.INT64.name()).setMode(mode.name());
            case LONG:
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return fieldSchema.setType(TableRowFieldType.TIMESTAMP.name()).setMode(mode.name());
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return fieldSchema.setType(TableRowFieldType.TIME.name()).setMode(mode.name());
                }
                return fieldSchema.setType(TableRowFieldType.INT64.name()).setMode(mode.name());
            case FLOAT:
            case DOUBLE:
                return fieldSchema.setType(TableRowFieldType.FLOAT64.name()).setMode(mode.name());
            case BOOLEAN:
                return fieldSchema.setType(TableRowFieldType.BOOL.name()).setMode(mode.name());
            case MAP:
                return fieldSchema.setType(TableRowFieldType.STRUCT.name()).setMode(mode.name());
            case UNION:
                for (final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return addMapValueType(fieldSchema, childSchema, TableRowFieldMode.NULLABLE);
                }
                throw new IllegalArgumentException();
            case RECORD:
                return fieldSchema.setType(TableRowFieldType.STRUCT.name()).setMode(mode.name());
            case ARRAY:
                return addMapValueType(fieldSchema, schema.getElementType(), mode);
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
