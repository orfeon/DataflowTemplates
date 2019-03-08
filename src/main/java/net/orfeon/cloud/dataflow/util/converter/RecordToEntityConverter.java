package net.orfeon.cloud.dataflow.util.converter;

import com.google.cloud.Date;
import com.google.datastore.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.util.Timestamps;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

public class RecordToEntityConverter {

    private static final int MAX_STRING_SIZE_BYTES = 1500;

    private static final Logger LOG = LoggerFactory.getLogger(RecordToEntityConverter.class);

    public static Entity convert(GenericRecord record, String kind, String keyField, int depth) {
        Entity.Builder builder = Entity.newBuilder();
        for(final Schema.Field field : record.getSchema().getFields()) {
            builder = setFieldValue(builder, field.name(), field.schema(), record, kind, keyField, depth);
        }

        final String keyName;
        try {
            if(depth == 0) {
                keyName = record.get(keyField).toString();
                final Key.Builder keyBuilder = Key.newBuilder()
                        .addPath(Key.PathElement.newBuilder().setKind(kind).setName(keyName).build());
                builder.setKey(keyBuilder);
            }
            return builder.build();
        } catch (Exception e) {
            LOG.error(String.format("Failed to get key name: %s", keyField));
            LOG.error(record.toString());
            throw e;
        }
    }

    public static Entity convert(SchemaAndRecord record, ValueProvider<String> kind, ValueProvider<String> keyField) {
        return convert(record.getRecord(), kind.get(), keyField.get(), 0);
    }

    private static Entity.Builder setFieldValue(Entity.Builder builder, String fieldName, Schema schema, GenericRecord record, String kind, String keyField, int depth) {
        if(record.get(fieldName) == null) {
            return builder.putProperties(fieldName, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        }
        switch (schema.getType()) {
            case STRING:
                final String stringValue = record.get(fieldName).toString();
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setStringValue(stringValue)
                        .setExcludeFromIndexes(stringValue.getBytes().length > MAX_STRING_SIZE_BYTES)
                        .build());
            case BYTES:
                final int precision = schema.getObjectProp("precision") != null ? Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
                final int scale = schema.getObjectProp("scale") != null ? Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
                final ByteBuffer bytes = (ByteBuffer)record.get(fieldName);
                if(LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    final String strValue = convertNumericBytesToString(bytes.array(), scale);
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setStringValue(strValue)
                            .build());
                }
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setBlobValue(ByteString.copyFrom(bytes))
                        .build());
            case ENUM:
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setStringValue(record.get(fieldName).toString())
                        .build());
            case INT:
                final Long intvalue = new Long((Integer)record.get(fieldName));
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    final LocalDate ld = LocalDate.ofEpochDay(intvalue);
                    final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setStringValue(date.toString())
                            .build());
                }
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setIntegerValue(intvalue)
                        .build());
            case LONG:
                final Long longvalue = (Long)record.get(fieldName);
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final Long microseconds = schema.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longvalue : longvalue * 1000;
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setTimestampValue(Timestamps.fromMicros(microseconds))
                            .build());
                }
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setIntegerValue(longvalue)
                        .build());
            case FLOAT:
            case DOUBLE:
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setDoubleValue((Double)record.get(fieldName))
                        .build());
            case BOOLEAN:
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setBooleanValue((Boolean)record.get(fieldName))
                        .build());
            case FIXED:
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setStringValue(record.get(fieldName).toString())
                        .build());
            case RECORD:
                final Entity childEntity = convert((GenericRecord)record.get(fieldName), kind, keyField, depth + 1);
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setEntityValue(childEntity).build());
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setFieldValue(builder, fieldName, childSchema, record, kind, keyField, depth);
                }
                return builder;
            case ARRAY:
                return setArrayFieldValue(builder, fieldName, schema.getElementType(), record, kind, keyField, depth);
            case NULL:
                return builder.putProperties(fieldName, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            default:
                return builder;
        }
    }

    private static Entity.Builder setArrayFieldValue(Entity.Builder builder, String fieldName, Schema schema, GenericRecord record, String kind, String keyField, int depth) {
        if(record.get(fieldName) == null) {
            return builder.putProperties(fieldName, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        }
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                final ArrayValue stringArray = ArrayValue.newBuilder().addAllValues(((List<Object>)record.get(fieldName)).stream()
                        .map(s -> Value.newBuilder().setStringValue(s.toString()).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setArrayValue(stringArray).build());
            case BYTES:
                final int precision = schema.getObjectProp("precision") != null ? Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
                final int scale = schema.getObjectProp("scale") != null ? Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    final ArrayValue numericArray = ArrayValue.newBuilder().addAllValues(((List<ByteBuffer>)record.get(fieldName)).stream()
                            .map(bytes -> convertNumericBytesToString(bytes.array(), scale))
                            .map(str -> Value.newBuilder().setStringValue(str).build())
                            .collect(Collectors.toList())).build();
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setArrayValue(numericArray).build());
                }
                final ArrayValue byteArray = ArrayValue.newBuilder().addAllValues(((List<ByteBuffer>)record.get(fieldName)).stream()
                        .map(ByteString::copyFrom)
                        .map(bstr -> Value.newBuilder().setBlobValue(bstr).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setArrayValue(byteArray).build());
            case INT:
                final List<Integer> intValues =  ((List<Integer>) record.get(fieldName));
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    final List<Value> dateList = intValues.stream()
                            .filter(days -> days != null)
                            .map(days -> LocalDate.ofEpochDay(days))
                            .map(localDate -> Date.fromYearMonthDay(localDate.getYear(), localDate.getMonth().getValue(), localDate.getDayOfMonth()))
                            .map(date -> Value.newBuilder().setStringValue(date.toString()).build())
                            .collect(Collectors.toList());
                    final ArrayValue dateArray = ArrayValue.newBuilder().addAllValues(dateList).build();
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setArrayValue(dateArray).build());
                } else {
                    final ArrayValue intArray = ArrayValue.newBuilder().addAllValues(intValues.stream()
                            .map(intValue -> Value.newBuilder().setIntegerValue(intValue).build())
                            .collect(Collectors.toList())).build();
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setArrayValue(intArray).build());
                }
            case LONG:
                final List<Long> longValues = (List<Long>)record.get(fieldName);
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final List<Value> timestampList = longValues.stream()
                            .filter(longValue -> longValue != null)
                            .map(longValue -> schema.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longValue : longValue * 1000)
                            .map(microSeconds -> Value.newBuilder().setTimestampValue(Timestamps.fromMicros(microSeconds)).build())
                            .collect(Collectors.toList());
                    final ArrayValue timestampArray = ArrayValue.newBuilder().addAllValues(timestampList).build();
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setArrayValue(timestampArray).build());
                } else {
                    final ArrayValue longArray = ArrayValue.newBuilder().addAllValues(longValues.stream()
                            .map(longValue -> Value.newBuilder().setIntegerValue(longValue).build())
                            .collect(Collectors.toList())).build();
                    return builder.putProperties(fieldName, Value.newBuilder()
                            .setArrayValue(longArray).build());
                }
            case FLOAT:
            case DOUBLE:
                final ArrayValue doubleArray = ArrayValue.newBuilder().addAllValues(((List<Double>)record.get(fieldName)).stream()
                        .map(floatValue -> Value.newBuilder().setDoubleValue(floatValue).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setArrayValue(doubleArray).build());
            case BOOLEAN:
                final ArrayValue booleanArray = ArrayValue.newBuilder().addAllValues(((List<Boolean>)record.get(fieldName)).stream()
                        .map(bool -> Value.newBuilder().setBooleanValue(bool).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setArrayValue(booleanArray).build());
            case FIXED:
                final ArrayValue fixedArray = ArrayValue.newBuilder().addAllValues(((List<Object>)record.get(fieldName)).stream()
                        .map(fixed -> Value.newBuilder().setStringValue(fixed.toString()).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setArrayValue(fixedArray).build());
            case RECORD:
                final ArrayValue entityArray = ArrayValue.newBuilder().addAllValues(((List<GenericRecord>)record.get(fieldName)).stream()
                        .map(childRecord -> Value.newBuilder().setEntityValue(convert(childRecord, kind, keyField, depth + 1)).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(fieldName, Value.newBuilder()
                        .setArrayValue(entityArray).build());
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setArrayFieldValue(builder, fieldName, childSchema, record, kind, keyField, depth);
                }
                return builder;
            case ARRAY:
                // BigQuery does not support nested array.
                return builder;
            case NULL:
                return builder.putProperties(fieldName, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            default:
                return builder;
        }
    }

    private static String convertNumericBytesToString(byte[] bytes, int scale) {
        BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

}
