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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RecordToEntityConverter {

    private static final int MAX_STRING_SIZE_BYTES = 1500;

    private static final Logger LOG = LoggerFactory.getLogger(RecordToEntityConverter.class);

    public static Entity convert(GenericRecord record, String kind, String keyField, int depth) {
        Entity.Builder builder = Entity.newBuilder();
        for(final Schema.Field field : record.getSchema().getFields()) {
            builder = setFieldValue(builder, field, field.schema(), record, kind, keyField, depth);
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


    private static Entity.Builder setFieldValue(Entity.Builder builder, Schema.Field field, Schema type, GenericRecord record, String kind, String keyField, int depth) {
        if(record.get(field.name()) == null) {
            return builder.putProperties(field.name(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        }
        switch (type.getType()) {
            case STRING:
                final String stringValue = record.get(field.name()).toString();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setStringValue(stringValue)
                        .setExcludeFromIndexes(stringValue.getBytes().length > MAX_STRING_SIZE_BYTES)
                        .build());
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                final ByteBuffer bytes = (ByteBuffer)record.get(field.name());
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    final String strValue = convertNumericBytesToString(bytes.array(), scale);
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setStringValue(strValue)
                            .build());
                }
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setBlobValue(ByteString.copyFrom(bytes))
                        .build());
            case ENUM:
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setStringValue(record.get(field.name()).toString())
                        .build());
            case INT:
                final Long intvalue = new Long((Integer)record.get(field.name()));
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    final LocalDate ld = LocalDate.ofEpochDay(intvalue);
                    final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setStringValue(date.toString())
                            .build());
                }
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setIntegerValue(intvalue)
                        .build());
            case LONG:
                final Long longvalue = (Long)record.get(field.name());
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    final Long microseconds = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longvalue : longvalue * 1000;
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setTimestampValue(Timestamps.fromMicros(microseconds))
                            .build());
                }
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setIntegerValue(longvalue)
                        .build());
            case FLOAT:
            case DOUBLE:
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setDoubleValue((Double)record.get(field.name()))
                        .build());
            case BOOLEAN:
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setBooleanValue((Boolean)record.get(field.name()))
                        .build());
            case FIXED:
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setStringValue(record.get(field.name()).toString())
                        .build());
            case RECORD:
                final Entity childEntity = convert((GenericRecord)record.get(field.name()), kind, keyField, depth + 1);
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setEntityValue(childEntity).build());
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : field.schema().getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return setFieldValue(builder, field, childSchema, record, kind, keyField, depth);
                }
                return builder;
            case ARRAY:
                if (Schema.Type.UNION.equals(field.schema().getType())) {
                    for(final Schema childSchema : field.schema().getTypes()) {
                        if (Schema.Type.NULL.equals(childSchema.getType())) {
                            continue;
                        }
                        return setArrayFieldValue(builder, field, childSchema.getElementType(), record, kind, keyField, depth);
                    }
                } else {
                    return setArrayFieldValue(builder, field, field.schema().getElementType(), record, kind, keyField, depth);
                }
            case NULL:
                return builder.putProperties(field.name(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            default:
                return builder;
        }
    }

    private static Entity.Builder setArrayFieldValue(Entity.Builder builder, Schema.Field field, Schema type, GenericRecord record, String kind, String keyField, int depth) {
        if(record.get(field.name()) == null) {
            return builder.putProperties(field.name(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        }
        switch (type.getType()) {
            case STRING:
                ArrayValue stringArray = ArrayValue.newBuilder().addAllValues(((List<Object>)record.get(field.name()))
                        .stream()
                        .map(s -> Value.newBuilder().setStringValue(s.toString()).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setArrayValue(stringArray).build());
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    ArrayValue numericArray = ArrayValue.newBuilder().addAllValues(((List<ByteBuffer>)record.get(field.name()))
                            .stream()
                            .map(bytes -> convertNumericBytesToString(bytes.array(), scale))
                            .map(str -> Value.newBuilder().setStringValue(str).build())
                            .collect(Collectors.toList())).build();
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setArrayValue(numericArray).build());
                }
                ArrayValue byteArray = ArrayValue.newBuilder().addAllValues(((List<ByteBuffer>)record.get(field.name()))
                        .stream()
                        .map(ByteString::copyFrom)
                        .map(bstr -> Value.newBuilder().setBlobValue(bstr).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setArrayValue(byteArray).build());
            case ENUM:
                ArrayValue enumArray = ArrayValue.newBuilder().addAllValues(((List<Object>)record.get(field.name()))
                        .stream()
                        .map(str -> Value.newBuilder().setStringValue(str.toString()).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setArrayValue(enumArray).build());
            case INT:
                final List<Integer> intvalues =  ((List<Integer>) record.get(field.name()));
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    final List<Value> dateList = new ArrayList<>();
                    for(final Integer days : intvalues) {
                        if(days == null) {
                            continue;
                        }
                        final LocalDate ld = LocalDate.ofEpochDay(days);
                        final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                        dateList.add(Value.newBuilder().setStringValue(date.toString()).build());
                    }
                    ArrayValue dateArray = ArrayValue.newBuilder().addAllValues(dateList).build();
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setArrayValue(dateArray).build());
                } else {
                    ArrayValue intArray = ArrayValue.newBuilder().addAllValues(intvalues
                            .stream()
                            .map(l -> Value.newBuilder().setIntegerValue(l).build())
                            .collect(Collectors.toList())).build();
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setArrayValue(intArray).build());
                }
            case LONG:
                final List<Long> longvalues = (List<Long>)record.get(field.name());
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    final List<Value> timestampList = new ArrayList<>();
                    for(final Long longvalue : longvalues) {
                        if(longvalue == null) {
                            continue;
                        }
                        final Long microseconds = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longvalue : longvalue * 1000;
                        timestampList.add(Value.newBuilder().setTimestampValue(Timestamps.fromMicros(microseconds)).build());
                    }
                    ArrayValue longArray = ArrayValue.newBuilder().addAllValues(timestampList).build();
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setArrayValue(longArray).build());
                } else {
                    ArrayValue longArray = ArrayValue.newBuilder().addAllValues(longvalues
                            .stream()
                            .map(l -> Value.newBuilder().setIntegerValue(l).build())
                            .collect(Collectors.toList())).build();
                    return builder.putProperties(field.name(), Value.newBuilder()
                            .setArrayValue(longArray).build());
                }
            case FLOAT:
            case DOUBLE:
                ArrayValue doubleArray = ArrayValue.newBuilder().addAllValues(((List<Double>)record.get(field.name()))
                        .stream()
                        .map(f -> Value.newBuilder().setDoubleValue(f).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setArrayValue(doubleArray).build());
            case BOOLEAN:
                ArrayValue booleanArray = ArrayValue.newBuilder().addAllValues(((List<Boolean>)record.get(field.name()))
                        .stream()
                        .map(bool -> Value.newBuilder().setBooleanValue(bool).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setArrayValue(booleanArray).build());
            case FIXED:
                ArrayValue fixedArray = ArrayValue.newBuilder().addAllValues(((List<Object>)record.get(field.name()))
                        .stream()
                        .map(s -> Value.newBuilder().setStringValue(s.toString()).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setArrayValue(fixedArray).build());
            case RECORD:
                ArrayValue entityArray = ArrayValue.newBuilder().addAllValues(((List<GenericRecord>)record.get(field.name()))
                        .stream()
                        .map(e -> Value.newBuilder().setEntityValue(convert(e, kind, keyField, depth + 1)).build())
                        .collect(Collectors.toList())).build();
                return builder.putProperties(field.name(), Value.newBuilder()
                        .setArrayValue(entityArray).build());
            case MAP:
                return builder;
            case UNION:
                for(final Schema childSchema : field.schema().getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    for(final Schema cchildSchema : childSchema.getElementType().getTypes()) {
                        if(Schema.Type.NULL.equals(cchildSchema.getType())) {
                            continue;
                        }
                        builder = setArrayFieldValue(builder, field, cchildSchema, record, kind, keyField, depth);
                        return builder;
                    }
                }
                return builder;
            case ARRAY:
                // BigQuery does not support nested array.
                return builder;
            case NULL:
                return builder.putProperties(field.name(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
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
